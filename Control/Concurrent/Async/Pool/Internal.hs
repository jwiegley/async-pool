{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Concurrent.Async.Pool.Internal where

import           Control.Applicative (Applicative((<*>), pure), (<$>))
import           Control.Arrow (first)
import           Control.Concurrent (ThreadId)
import qualified Control.Concurrent.Async as Async (withAsync)
import           Control.Concurrent.Async.Pool.Async
import           Control.Concurrent.STM
import           Control.Exception (SomeException, throwIO, finally)
import           Control.Monad hiding (forM, forM_)
import           Control.Monad.Base
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Trans.Control
import           Data.Foldable (Foldable(foldMap), toList, forM_, all)
import           Data.Graph.Inductive.Graph as Gr (Graph(empty))
import           Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap
import           Data.List (delete)
import           Data.Monoid (Monoid(mempty), (<>))
import           Data.Traversable (Traversable(sequenceA), forM)
import           Prelude hiding (mapM_, mapM, foldr, all, any, concatMap, foldl1)
import           Unsafe.Coerce

-- | Return a list of actions ready for execution, by checking the graph to
--   ensure all dependencies have completed.
getReadyNodes :: TaskGroup -> TaskGraph -> STM (IntMap (IO ThreadId, TMVar a))
getReadyNodes p g = do
    availSlots <- readTVar (avail p)
    check (availSlots > 0)
    taskQueue  <- readTVar (pending p)
    check (not (IntMap.null taskQueue))
    let readyNodes = IntMap.fromList
                   . take availSlots
                   . IntMap.toAscList
                   . IntMap.filterWithKey (const . isReady)
                   $ taskQueue
    check (not (IntMap.null readyNodes))
    writeTVar (avail p) (availSlots - IntMap.size readyNodes)
    writeTVar (pending p) (taskQueue IntMap.\\ readyNodes)
    return readyNodes
  where
    isReady = all isCompleted . inn g

    isCompleted (_, _, Completed) = True
    isCompleted (_, _, _)         = False

-- | Return a list of tasks ready to execute, and their related state
--   variables from the dependency graph.
getReadyTasks :: TaskGroup -> STM [(TVar State, (IO ThreadId, TMVar a))]
getReadyTasks p = do
    g <- readTVar (tasks (pool p))
    map (first (getTaskVar g)) . IntMap.toList <$> getReadyNodes p g

-- | Create a task pool for managing many-to-many acyclic dependencies among
--   tasks.
createPool :: IO Pool
createPool = Pool <$> newTVarIO Gr.empty
                  <*> newTVarIO 0

-- | Use a task pool for a bounded region. At the end of the region,
-- 'withPool' will block until all tasks have completed.
withPool :: (Pool -> IO a) -> IO a
withPool f = do
    p <- createPool
    x <- f p
    atomically $ syncPool p
    return x

-- | Create a task group for executing interdependent tasks concurrently.  The
--   number of available slots governs how many tasks may run at one time.
createTaskGroup :: Pool -> Int -> IO TaskGroup
createTaskGroup p cnt = do
    c <- newTVarIO cnt
    m <- newTVarIO IntMap.empty
    -- Prior to GHC 8, this call to unsafeCoerce was not necessary.
    return $ TaskGroup p c (unsafeCoerce m)

-- | Execute tasks in a given task group.  The number of slots determines how
--   many threads may execute concurrently.
runTaskGroup :: TaskGroup -> IO ()
runTaskGroup p = forever $ do
    ready <- atomically $ do
        cnt <- readTVar (avail p)
        check (cnt > 0)
        ready <- getReadyTasks p
        check (not (null ready))
        forM_ ready $ \(tv, _) -> writeTVar tv Starting
        return ready
    forM_ ready $ \(tv, (go, var)) -> do
        t <- go
        atomically $ swapTVar tv $ Started t var

-- | Create a task group within the given pool having a specified number of
--   execution slots, but with a bounded lifetime.  Leaving the block cancels
--   every task still executing in the group.
withTaskGroupIn :: Pool -> Int -> (TaskGroup -> IO b) -> IO b
withTaskGroupIn p n f = createTaskGroup p n >>= \g ->
    Async.withAsync (runTaskGroup g) $ const $ f g `finally` cancelAll g

-- | Create both a pool, and a task group with a given number of execution slots,
--   but with a bounded lifetime. Once the given function exits, all tasks (that 
--   are still running) in the TaskGroup will be cancelled.
withTaskGroup :: Int -> (TaskGroup -> IO b) -> IO b
withTaskGroup n f = createPool >>= \p -> withTaskGroupIn p n f

-- | Given parent and child tasks, link them so the child cannot execute until
--   the parent has finished.
makeDependent :: Pool
              -> Handle    -- ^ Handle of task doing the waiting
              -> Handle    -- ^ Handle of task we must wait on (the parent)
              -> STM ()
makeDependent p child parent = do
    g <- readTVar (tasks p)
    -- Check whether the parent is in any way dependent on the child, which
    -- would introduce a cycle.
    when (gelem parent g) $
        case esp child parent g of
            -- If the parent is no longer in the graph, there is no need to
            -- establish a dependency.  The child can begin executing in the
            -- next free slot.
            [] -> modifyTVar (tasks p) (insEdge (parent, child, Pending))
            _  -> error "makeDependent: Cycle in task graph"

-- | Given parent and child tasks, link them so the child cannot execute until
--   the parent has finished.  This function does not check for introduction of
--   cycles into the dependency graph, which would prevent the child from ever
--   running.
unsafeMakeDependent :: Pool
                    -> Handle    -- ^ Handle of task doing the waiting
                    -> Handle    -- ^ Handle of task we must wait on (the parent)
                    -> STM ()
unsafeMakeDependent p child parent = do
    g <- readTVar (tasks p)
    -- If the parent is no longer in the graph, there is no need to establish
    -- dependency.  The child can begin executing in the next free slot.
    when (gelem parent g) $
        modifyTVar (tasks p) (insEdge (parent, child, Pending))

-- | Equivalent to 'async', but acts in STM so that 'makeDependent' may be
--   called after the task is created, but before it begins executing.
asyncSTM :: TaskGroup -> IO a -> STM (Async a)
asyncSTM p = asyncUsing p rawForkIO

-- | Submit a task which begins execution after all its parents have completed.
--   This is equivalent to submitting a new task with 'asyncSTM' and linking
--   it to its parents using 'mapM makeDependent'.
asyncAfterAll :: TaskGroup -> [Handle] -> IO a -> IO (Async a)
asyncAfterAll p parents t = atomically $ do
    child <- asyncUsing p rawForkIO t
    forM_ parents $ makeDependent (pool p) (taskHandle child)
    return child

-- | Submit a task that begins execution only after its parent has completed.
--   This is equivalent to submitting a new task with 'asyncSTM' and linking
--   it to its parent using 'makeDependent'.
asyncAfter :: TaskGroup -> Async b -> IO a -> IO (Async a)
asyncAfter p parent = asyncAfterAll p [taskHandle parent]

-- | Helper function used by several of the variants of 'mapTasks' below.
mapTasksWorker :: Traversable t
               => TaskGroup
               -> t (IO a)
               -> (IO (t b) -> IO (t c))
               -> (Async a -> IO b)
               -> IO (t c)
mapTasksWorker p fs f g = do
    hs <- forM fs $ atomically . asyncUsing p rawForkIO
    f $ forM hs g

-- | Execute a group of tasks within the given task group, returning the
--   results in order.  The order of execution is random, but the results are
--   returned in order.
mapTasks :: Traversable t => TaskGroup -> t (IO a) -> IO (t a)
mapTasks p fs = mapTasksWorker p fs id wait

-- | Execute a group of tasks within the given task group, returning the
--   results in order as an Either type to represent exceptions from actions.
--   The order of execution is random, but the results are returned in order.
mapTasksE :: Traversable t => TaskGroup -> t (IO a) -> IO (t (Either SomeException a))
mapTasksE p fs = mapTasksWorker p fs id waitCatch

-- | Execute a group of tasks within the given task group, ignoring results.
mapTasks_ :: Foldable t => TaskGroup -> t (IO a) -> IO ()
mapTasks_ p fs = forM_ fs $ atomically . asyncUsing p rawForkIO

-- | Execute a group of tasks within the given task group, ignoring results,
--   but returning a list of all exceptions.
mapTasksE_ :: Traversable t => TaskGroup -> t (IO a) -> IO (t (Maybe SomeException))
mapTasksE_ p fs = mapTasksWorker p fs (fmap (fmap leftToMaybe)) waitCatch
  where
    leftToMaybe :: Either a b -> Maybe a
    leftToMaybe = either Just (const Nothing)

-- | Execute a group of tasks, but return the first result or failure and
--   cancel the remaining tasks.
mapRace :: Foldable t
        => TaskGroup -> t (IO a) -> IO (Async a, Either SomeException a)
mapRace p fs = do
    hs <- atomically $ sequenceA $ foldMap ((:[]) <$> asyncUsing p rawForkIO) fs
    waitAnyCatchCancel hs

-- | Given a list of actions yielding 'Monoid' results, execute the actions
--   concurrently (up to N at a time, based on available slots), and 'mappend'
--   each pair of results concurrently as they become ready.  The immediate
--   result of this function is an 'Async' representing the final value.
--
--   This is similar to the following: @mconcat <$> mapTasks n actions@,
--   except that intermediate results can be garbage collected as soon as
--   they've been merged.  Also, the value returned from this function is an
--   'Async' which may be polled for the final result.
--
--   Lastly, if an 'Exception' occurs in any subtask, the final result will
--   also yield an exception -- but not necessarily the first or last that was
--   caught.
mapReduce :: (Foldable t, Monoid a)
          => TaskGroup     -- ^ Task group to execute the tasks within
          -> t (IO a)      -- ^ Set of Monoid-yielding IO actions
          -> STM (Async a) -- ^ Returns the final result task
mapReduce p fs = do
    -- Submit all the tasks right away, and jobs to combine all those results.
    -- Since we're working with a Monoid, it doesn't matter what order they
    -- complete in, or what order we combine the results in, just as long we
    -- each combination waits on the results it intends to combine.
    hs <- sequenceA $ foldMap ((:[]) <$> asyncUsing p rawForkIO) fs
    loopM hs
  where
    loopM hs = do
        hs' <- squeeze hs
        case hs' of
            []  -> error "mapReduce: impossible"
            [x] -> return x
            xs  -> loopM xs

    squeeze []  = (:[]) <$> asyncUsing p rawForkIO (return mempty)
    squeeze [x] = return [x]
    squeeze (x:y:xs) = do
        t <- asyncUsing p rawForkIO $ do
            meres <- atomically $ do
                -- These polls should by definition always succeed, since this
                -- task should not start until results are available.
                eres1 <- pollSTM x
                eres2 <- pollSTM y
                case liftM2 (<>) <$> eres1 <*> eres2 of
                    Nothing -> retry
                    Just a  -> return a
            case meres of
                Left e  -> throwIO e
                Right a -> return a
        forM_ [x, y] (unsafeMakeDependent (pool p) (taskHandle t) . taskHandle)
        case xs of
            [] -> return [t]
            _  -> (t :) <$> squeeze xs

-- | Execute a group of tasks concurrently (using up to N active threads,
--   depending on the task group), and feed results to a continuation as soon
--   as they become available, in random order.  The continuation function may
--   return a monoid value which is accumulated to yield a final result.  If
--   no such value is needed, simply provide `()`.
scatterFoldMapM :: (Foldable t, Monoid b, MonadBaseControl IO m)
                => TaskGroup -> t (IO a) -> (Either SomeException a -> m b) -> m b
scatterFoldMapM p fs f = do
    hs <- liftBase $ atomically
                  $ sequenceA
                  $ foldMap ((:[]) <$> asyncUsing p rawForkIO) fs
    control $ \(run :: m b -> IO (StM m b)) -> loop run (run $ return mempty) (toList hs)
  where
    loop _ z [] = z
    loop run z hs = do
        (h, eres) <- atomically $ do
            mres <- foldM go Nothing hs
            maybe retry return mres
        r' <- z
        r  <- run $ do
            s <- restoreM r'
            r <- f eres
            return $ s <> r
        loop run (return r) (delete h hs)

    go acc@(Just _) _ = return acc
    go acc h = do
        eres <- pollSTM h
        return $ case eres of
            Nothing        -> acc
            Just (Left e)  -> Just (h, Left e)
            Just (Right x) -> Just (h, Right x)

-- | maps an @IO@-performing function over any @Traversable@ data
-- type, performing all the @IO@ actions concurrently, and returning
-- the original data structure with the arguments replaced by the
-- results.
--
-- For example, @mapConcurrently@ works with lists:
--
-- > pages <- mapConcurrently getURL ["url1", "url2", "url3"]
--
mapConcurrently :: Traversable t => TaskGroup -> (a -> IO b) -> t a -> IO (t b)
mapConcurrently tg f = mapTasks tg . fmap f

-- | The 'Task' Applicative and Monad allow for task dependencies to be built
--   using Applicative and do notation.  Monadic evaluation is sequenced,
--   while applicative Evaluation is concurrent for each argument.  In this
--   way, mixing the two builds a dependency graph via ordinary Haskell code.
newtype Task a = Task { runTask' :: TaskGroup -> IO (IO a) }

-- | Run a value in the 'Task' monad and block until the final result is
--   computed.
runTask :: TaskGroup -> Task a -> IO a
runTask group ts = join $ runTask' ts group

-- | Lift any 'IO' action into a 'Task'.  This is a synonym for 'liftIO'.
task :: IO a -> Task a
task action = Task $ \_ -> return action

instance Functor Task where
    fmap f (Task k) = Task $ fmap (fmap (liftM f)) k

instance Applicative Task where
    pure x = Task $ \_ -> return (return x)
    Task f <*> Task x = Task $ \tg -> do
        xa <- x tg
        x' <- wait <$> async tg xa
        fa <- f tg
        return $ fa <*> x'

instance Monad Task where
    return = pure
    Task m >>= f = Task $ \tg -> join (m tg) >>= flip runTask' tg . f

instance MonadIO Task where
    liftIO = task
