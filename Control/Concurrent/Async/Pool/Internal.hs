{-# LANGUAGE RankNTypes #-}

module Control.Concurrent.Async.Pool.Internal where

import           Control.Applicative
import           Control.Arrow (first)
import           Control.Concurrent
import qualified Control.Concurrent.Async as Async
import           Control.Concurrent.Async.Pool.Async
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad hiding (forM, forM_, mapM, mapM_)
import           Control.Monad.IO.Class
import           Data.Foldable
import           Data.Graph.Inductive.Graph as Gr hiding ((&))
import           Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap
import           Data.List (delete)
import           Data.Monoid
import           Data.Traversable
import           Prelude hiding (mapM_, mapM, foldr, all, any, concatMap, foldl1)

-- | Return a list of unlabeled nodes ready for execution.  This decreases the
--   number of available slots, but does not remove the nodes from the graph.
getReadyNodes :: TaskGroup -> TaskGraph -> STM (IntMap (IO ThreadId))
getReadyNodes p g = do
    availSlots <- readTVar (avail p)
    check (availSlots > 0)
    taskQueue  <- readTVar (pending p)
    check (not (IntMap.null taskQueue))
    let readyNodes = IntMap.fromList $ take availSlots $ IntMap.toAscList
                   $ IntMap.filterWithKey (const . isReady) taskQueue
    check (not (IntMap.null readyNodes))
    writeTVar (avail p) (availSlots - IntMap.size readyNodes)
    writeTVar (pending p) (taskQueue IntMap.\\ readyNodes)
    return readyNodes
  where
    -- Returns True for every node for which there are no dependencies or
    -- incomplete dependencies, and which is not itself a completed dependency.
    -- The reason for the latter condition is that we keep completed nodes
    -- with dependents in graph until their dependents have completed
    -- (recursively), so that the dependent knows only to begin when its
    -- parent has truly finished -- a fact which cannot be determined using
    -- only the process map.
    isReady x = all      isCompleted  (inn g x)
              && all (not . isCompleted) (out g x)

    isCompleted (_, _, Completed) = True
    isCompleted (_, _, _)         = False

-- | Return information about the list of tasks ready to execute, sufficient
--   to start them and remove them from the graph afterwards.
getReadyTasks :: TaskGroup -> STM [(TMVar State, IO ThreadId)]
getReadyTasks p = do
    g <- readTVar (tasks (pool p))
    map (first (getTaskVar g)) . IntMap.toList <$> getReadyNodes p g

-- | Begin executing tasks in the given pool.  The number of slots determines
--   how many threads may execute concurrently.  This number is adjustable
--   dynamically, by calling 'setPoolSlots', though reducing it does not cause
--   already active threads to stop.
runTaskGroup :: TaskGroup -> IO ()
runTaskGroup p = forever $ do
    ready <- atomically $ do
        cnt <- readTVar (avail p)
        check (cnt > 0)
        ready <- getReadyTasks p
        check (not (null ready))
        forM_ ready $ \(tv, _) -> putTMVar tv Starting
        return ready

    forM_ ready $ \(tv, go) -> do
        t <- go
        atomically $ swapTMVar tv $ Started t

-- | Create a task pool for managing many-to-many acyclic dependencies among
--   tasks.
createPool :: IO Pool
createPool =
    Pool <$> newTVarIO Gr.empty
         <*> newTVarIO 0

-- | Create a task group for executing interdependent tasks concurrently.  The
--   number of available slots governs how many tasks may run at one time.
createTaskGroup :: Pool -> Int -> IO TaskGroup
createTaskGroup p cnt =
    TaskGroup <$> pure p
              <*> newTVarIO cnt
              <*> newTVarIO mempty

-- | Given parent and child task handles, link them so that the child cannot
--   execute until the parent has finished.  This does not check for cycles, so
--   can add new tasks in the time it takes to add a node and an edge into the
--   graph.
unsafeMakeDependent :: Pool
                    -> Handle    -- ^ Task doing the waiting
                    -> Handle    -- ^ Task we must wait on (the parent)
                    -> STM ()
unsafeMakeDependent p child parent = do
    g <- readTVar (tasks p)
    -- If the parent is no longer in the graph, there is no need to establish
    -- a dependency.  The child can begin executing in the next free slot.
    when (gelem parent g) $
        modifyTVar (tasks p) (insEdge (parent, child, Pending))

makeDependent :: Pool
              -> Handle    -- ^ Task doing the waiting
              -> Handle    -- ^ Task we must wait on (the parent)
              -> STM ()
makeDependent p child parent = do
    g <- readTVar (tasks p)
    -- Check whether the parent is in any way dependent on the child, which
    -- would introduce a cycle.
    case esp child parent g of
        -- If the parent is no longer in the graph, there is no need to
        -- establish a dependency.  The child can begin executing in the next
        -- free slot.
        [] -> when (gelem parent g) $
                 modifyTVar (tasks p) (insEdge (parent, child, Pending))

        _ -> error "sequenceTasks: Attempt to introduce cycle in task graph"

-- | Submit a task, but only allow it begin executing once its parent task has
--   completed.  This is equivalent to submitting a new task and linking it to
--   its parent using 'sequenceTasks' within a single STM transaction.
asyncAfter :: TaskGroup -> [Handle] -> IO a -> IO (Async a)
asyncAfter p parents t = atomically $ do
    child <- asyncUsing p rawForkIO t
    forM_ parents $ makeDependent (pool p) (taskHandle child)
    return child

-- | Execute an IO action, passing it a running pool with N available slots.
withTaskGroup :: Int -> (TaskGroup -> IO b) -> IO b
withTaskGroup n f = do
    p <- createPool
    g <- createTaskGroup p n
    Async.withAsync (runTaskGroup g) $ const $ f g `finally` cancelAll g

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

-- | Run a group of up to N tasks at a time concurrently, returning the
--   results in order.  The order of execution is random, but the results are
--   returned in order.  This version is more optimal than mapTasks in the
--   case where the Pool type is the same as the result type of each action.
mapTasks :: Traversable t => TaskGroup -> t (IO a) -> IO (t a)
mapTasks p fs = mapTasksWorker p fs id wait

-- -- | Run a group of up to N tasks at a time concurrently, returning the
-- --   results in order.  The order of execution is random, but the results are
-- --   returned in order.
mapTasksE :: Traversable t => TaskGroup -> t (IO a) -> IO (t (Either SomeException a))
mapTasksE p fs = mapTasksWorker p fs id waitCatch

-- | Run a group of up to N tasks at a time concurrently, ignoring the
--   results.
mapTasks_ :: Foldable t => TaskGroup -> t (IO a) -> IO ()
mapTasks_ p fs = forM_ fs $ atomically . asyncUsing p rawForkIO

-- -- | Run a group of up to N tasks at a time concurrently, ignoring the
-- --   results, but returning whether an exception occurred for each task.
mapTasksE_ :: Traversable t => TaskGroup -> t (IO a) -> IO (t (Maybe SomeException))
mapTasksE_ p fs = mapTasksWorker p fs (fmap (fmap leftToMaybe)) waitCatch
  where
    leftToMaybe :: Either a b -> Maybe a
    leftToMaybe = either Just (const Nothing)

-- | Execute a group of tasks (where only N tasks at most may run,
--   corresponding to the number of available slots in the pool), returning
--   the first result or failure.  'Nothing' is returned if no tasks were
--   provided.
mapRace :: Foldable t
        => TaskGroup -> t (IO a) -> IO (Async a, Either SomeException a)
mapRace p fs = do
    hs <- atomically $ sequenceA $ foldMap ((:[]) <$> asyncUsing p rawForkIO) fs
    waitAnyCatchCancel hs

-- | Given a list of actions yielding 'Monoid' results, execute the actions
--   concurrently (up to N at time, based on available slots), and also
--   mappend each pair of results concurrently as they become ready.
--
--   The immediate result from this function is Handle representing the final
--   task -- dependent on all the rest -- whose value is the final, aggregate
--   result.
--
--   This is equivalent to the following: @mconcat <$> mapTasks n actions@,
--   except that intermediate results can be garbage collected as soon as
--   they've merged.  Also, the value returned from this function is a
--   'Handle' which may be polled until that final result is ready.
--
--   Lastly, if any Exception occurs, the result obtained from waiting on or
--   polling the Handle will be one of those exceptions, but not necessarily the
--   first or the last.
mapReduce :: (Foldable t, Monoid a)
          => TaskGroup     -- ^ Pool to execute the tasks within
          -> t (IO a)      -- ^ Set of Monoid-yielding IO actions
          -> STM (Async a) -- ^ Returns a Handle to the final result task
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
--   depending on the pool), and feed results to the continuation immediately
--   as they become available, in whatever order they complete in.  That
--   function may return a monoid value which is accumulated to yield the
--   final result.
scatterFoldM :: (Foldable t, Monoid b)
             => TaskGroup -> t (IO a) -> (Either SomeException a -> IO b) -> IO b
scatterFoldM p fs f = do
    hs <- atomically $ sequenceA $ foldMap ((:[]) <$> asyncUsing p rawForkIO) fs
    loop mempty (toList hs)
  where
    loop z [] = return z
    loop z hs = do
        (h, eres) <- atomically $ do
            mres <- foldM go Nothing hs
            maybe retry return mres
        r <- f eres
        loop (z <> r) (delete h hs)

    go acc@(Just _) _ = return acc
    go acc h = do
        eres <- pollSTM h
        return $ case eres of
            Nothing        -> acc
            Just (Left e)  -> Just (h, Left e)
            Just (Right x) -> Just (h, Right x)

-- | The 'Task' Applicative and Monad allow for task dependencies to be built
--   using applicative and do notation.  Monadic evaluation is sequenced,
--   while applicative evaluation is done concurrently for each argument.
newtype Task a = Task { runTask' :: TaskGroup -> IO (IO a) }

runTask :: TaskGroup -> Task a -> IO a
runTask group ts = join $ runTask' ts group

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