{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}

module Data.TaskPool.Internal where

import           Control.Applicative hiding (empty)
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad hiding (forM, forM_, mapM, mapM_)
import           Control.Monad.IO.Class
import           Data.Foldable
import           Data.Graph.Inductive.Graph as Gr hiding ((&))
import           Data.Graph.Inductive.PatriciaTree
import           Data.Graph.Inductive.Query.BFS
import           Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap
import           Data.List (delete)
import           Data.Maybe (catMaybes)
import           Data.Monoid
import           Data.Traversable
import           Data.Typeable
import           Prelude hiding (mapM_, mapM, foldr, all, any, concatMap,
                                 foldl1)
import           Unsafe.Coerce (unsafeCoerce)

-- | A 'Handle' is a unique identifier for a task submitted to a 'Pool'.
type Handle    = Node
type TaskVar a = TVar (State a)

data State a = Ready
            | Starting
            | Started (Async a)
            | Observed

instance Show (State a) where
    show Ready       = "Ready"
    show Starting    = "Starting"
    show (Started _) = "Started"
    show Observed    = "Observed"

data Status = Pending | Completed deriving (Eq, Show)

type TaskGraph a = Gr (TaskVar a) Status

data TaskException = TaskUnknown Handle
                   | TaskAlreadyObserved Handle
    deriving (Show, Typeable)

instance Exception TaskException

-- | A 'Pool' manages a collection of possibly interdependent tasks, such that
--   tasks await execution until the tasks they depend on have finished (and
--   tasks may depend on an arbitrary number of other tasks), while
--   independent tasks execute concurrently up to the number of available
--   resource slots in the pool.
--
--   Results from each task are available until the status of the task is
--   polled or waited on.  Further, the results are kept until that occurs, so
--   failing to ever wait will result in a memory leak.
--
--   Tasks may be cancelled, in which case all dependent tasks are
--   unscheduled.
data Pool a = Pool
    { slots :: TVar Int
      -- ^ The total number of execution slots in the pool.  If nothing is
      --   running, this is also the number of available slots.  This can be
      --   changed dynamically using 'setPoolSlots'.
    , avail :: TVar Int
      -- ^ The number of available execution slots in the pool.
    , pending :: TVar (IntMap (IO a))
      -- ^ Nodes in the task graph that are waiting to start.
    , tasks :: TVar (TaskGraph a)
      -- ^ The task graph represents a partially ordered set P with subset S
      --   such that for every x ∈ S and y ∈ P, either x ≤ y or x is unrelated
      --   to y.  Stated more simply, S is the set of least elements of all
      --   maximal chains in P.  In our case, ≤ relates two uncompleted tasks
      --   by dependency.  Therefore, S is equal to the set of tasks which may
      --   execute concurrently, as none of them have incomplete dependencies.
      --
      --   We use a graph representation to make determination of S more
      --   efficient (where S is just the set of roots in P expressed as a
      --   graph).  Completion status is recorded on the edges, and nodes are
      --   removed from the graph once no other incomplete node depends on
      --   them.
    , tokens :: TVar Int
      -- ^ Tokens identify tasks, and are provisioned monotonically.
    }

-- | Return a list of unlabeled nodes ready for execution.  This decreases the
--   number of available slots, but does not remove the nodes from the graph.
getReadyNodes :: Pool a -> TaskGraph a -> STM (IntMap (IO a))
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

-- | Given a task handle, return everything we know about that task.
getTaskVar :: TaskGraph a -> Handle -> TaskVar a
getTaskVar g h = let (_to, _, t, _from) = context g h in t

-- | Return information about the list of tasks ready to execute, sufficient
--   to start them and remove them from the graph afterwards.
getReadyTasks :: Pool a -> STM [(Handle, TaskVar a, IO a)]
getReadyTasks p = do
    g <- readTVar (tasks p)
    map (\(h, a) -> (h, getTaskVar g h, a)) . IntMap.toList
        <$> getReadyNodes p g

-- | Begin executing tasks in the given pool.  The number of slots determines
--   how many threads may execute concurrently.  This number is adjustable
--   dynamically, by calling 'setPoolSlots', though reducing it does not cause
--   already active threads to stop.
runPool :: Pool a -> IO ()
runPool p = forever $ do
    ready <- atomically $ do
        cnt <- readTVar (slots p)
        check (cnt > 0)
        ready <- getReadyTasks p
        check (not (null ready))
        forM_ ready $ \(_, tv, _) -> writeTVar tv Starting
        return ready
    forM_ ready $ \(h, tv, go) -> do
        x <- startTask p h go
        atomically $ modifyTVar tv $ \s ->
            case s of
                Starting -> Started x
                _ -> error $ "runPool: unexpected state " ++ show s

-- | Start a task within the given pool.  This begins execution as soon as the
--   runtime is able to.
startTask :: Pool a -> Handle -> IO a -> IO (Async a)
startTask p h go = async $ finally go $ atomically $ do
    ss <- readTVar (slots p)
    modifyTVar (avail p) $ \a -> min (succ a) ss
    cleanupTask p h

cleanupTask :: Pool a -> Handle -> STM ()
cleanupTask p h = do
    -- Once the task is done executing, we must alter the graph so any
    -- dependent children will know their parent has completed.
    g <- readTVar (tasks p)
    case zip (repeat h) (Gr.suc g h) of
        -- If nothing dependend on this task and if the final result value
        -- has been observed, prune it from the graph, as well as any
        -- parents which now have no dependents.  Otherwise mark the edges
        -- as Completed so dependent children can execute.
        [] -> do
            status <- readTVar (getTaskVar g h)
            case status of
                Ready    -> error "cleanupTask: impossible"
                Starting ->
                    -- If we are Starting (meaning we completed faster than
                    -- runPool could assign a Started state), or Started,
                    -- leave the task in the graph for the user to wait on
                    -- later.
                    return ()
                Started _ -> return ()
                Observed  -> writeTVar (tasks p) (dropTask h g)
        es -> writeTVar (tasks p) $ insEdges (completeEdges es)
                                 $ delEdges es g
  where
    completeEdges = map (\(f, t) -> (f, t, Completed))

    dropTask k gr = foldl' f (delNode k gr) (Gr.pre gr k)
      where
        f g n = if outdeg g n == 0 then dropTask n g else g

-- | Create a thread pool for executing interdependent tasks concurrently.
--   The number of available slots governs how many tasks may run at one time.
createPool :: Int                -- ^ Maximum number of running tasks.
           -> IO (Pool a)
createPool cnt = atomically $
    Pool <$> newTVar cnt
         <*> newTVar cnt
         <*> newTVar mempty
         <*> newTVar Gr.empty
         <*> newTVar 0

-- | Set the number of available execution slots in the given 'Pool'.
--   Increasing the number will cause waiting threads to start executing
--   immediately, while decreasing the number only decreases any available
--   slots -- it does not cancel already executing threads.
setPoolSlots :: Pool a -> Int -> STM ()
setPoolSlots p n = do
    ss <- readTVar (slots p)
    let diff = n - ss
    modifyTVar (avail p) (\x -> max 0 (x + diff))
    writeTVar (slots p) (max 0 n)

-- | Cancel every running thread in the pool and unschedule any that had not
--   begun yet.
cancelAll :: Pool a -> IO ()
cancelAll p = (mapM_ cancel =<<) $ atomically $ do
    writeTVar (pending p) IntMap.empty
    g  <- readTVar (tasks p)
    xs <- catMaybes <$> mapM (getTaskAsync g) (nodes g)
    writeTVar (tasks p) Gr.empty
    return xs

getTaskAsync :: TaskGraph a -> Node -> STM (Maybe (Async a))
getTaskAsync g h = do
    status <- readTVar (getTaskVar g h)
    -- If the task is in Starting state, it is about to be started in a small
    -- amount of time, so retry until we can get the Async from the Started
    -- state.  This allows cancellation of tasks which are just starting, but
    -- are not yet actually started.  If we did not retry, there would be a race
    -- condition for actions like process cancellation, since a Starting state
    -- has no async value, but the process to produce one is already underway.
    case status of
        Starting -> retry
        Started x -> return $ Just x
        _ -> return Nothing

-- | Cancel a task submitted to the pool.  This will unschedule it if it had not
--   begun yet, or cancel its thread if it had.
cancelTask :: Pool a -> Handle -> IO ()
cancelTask p h = (mapM_ cancel =<<) $ atomically $ do
    g <- readTVar (tasks p)
    let hs = if gelem h g then nodeList g h else []
    xs <- foldM (go g) [] hs
    writeTVar (tasks p) $ foldl' (flip delNode) g $ map fst xs
    return $ map snd xs
  where
    go g acc h' = do
        mres <- getTaskAsync g h'
        return $ case mres of
            Nothing -> acc
            Just x  -> (h', x) : acc

    nodeList :: TaskGraph a -> Node -> [Node]
    nodeList g k = k : concatMap (nodeList g) (Gr.suc g k)

-- | Return the next available thread identifier from the pool.  These are
--   monotonically increasing integers.
nextIdent :: Pool a -> STM Int
nextIdent p = do
    tok <- readTVar (tokens p)
    writeTVar (tokens p) (succ tok)
    return tok

-- | Submit an 'IO' action for execution within the managed thread pool.  When
--   it actually begins executes is determined by the number of available
--   slots, whether the threaded runtime is being used, and how long it takes
--   the jobs before it to complete.
submitTask :: Pool a -> IO a -> STM Handle
submitTask p action = do
    h <- nextIdent p
    modifyTVar (pending p) (IntMap.insert h action)
    tv <- newTVar Ready
    modifyTVar (tasks p) (insNode (h, tv))
    return h

-- | Submit an 'IO ()' action, where we will never care about the result value
--   or if an exception occurred within the task.  This means its process
--   table entry is automatically cleared immediately upon completion of the
--   task.  Use this if you are doing your own result propagation, such as
--   writing to a 'TChan' within the task.
submitTask_ :: Pool a -> IO a -> STM Handle
submitTask_ p action = do
    v <- newEmptyTMVar
    h <- submitTask p (go v)
    putTMVar v h
    return h
  where
    go v = do
        res <- action
        atomically $ do
            h <- takeTMVar v
            g <- readTVar (tasks p)
            let tv = getTaskVar g h
            status <- readTVar tv
            case status of
                Ready     -> error "submitTask_: impossible"
                -- Wait until we are beyond the Starting state, otherwise any
                -- change we make here will be overwritten.  There is only a
                -- single line of code distance between when the task is
                -- started, and when it gets placed into Started state, but
                -- it's still a possibility and so we retry until the state
                -- has been finalized as Started.
                Starting  -> retry
                -- Note: this writeTVar happens before the call to cleanupTask
                -- at the very end of the thread (as staged by startTask).
                Started _ -> writeTVar tv Observed
                Observed  -> return ()
        return res

-- | Given parent and child task handles, link them so that the child cannot
--   execute until the parent has finished.  This does not check for cycles, so
--   can add new tasks in the time it takes to add a node and an edge into the
--   graph.
unsafeSequenceTasks :: Pool a
                    -> Handle    -- ^ Task doing the waiting
                    -> Handle    -- ^ Task we must wait on (the parent)
                    -> STM ()
unsafeSequenceTasks p child parent = do
    g <- readTVar (tasks p)
    -- If the parent is no longer in the graph, there is no need to establish
    -- a dependency.  The child can begin executing in the next free slot.
    when (gelem parent g) $
        modifyTVar (tasks p) (insEdge (parent, child, Pending))

sequenceTasks :: Pool a
              -> Handle    -- ^ Task doing the waiting
              -> Handle    -- ^ Task we must wait on (the parent)
              -> STM ()
sequenceTasks p child parent = do
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
submitDependentTask :: Pool a -> [Handle] -> IO a -> STM Handle
submitDependentTask p parents t = do
    child <- submitTask p t
    forM_ parents $ sequenceTasks p child
    return child

-- | Submit a dependent task where we do not care about the result value or if
--   an exception occurred.  See 'submitTask_'.
submitDependentTask_ :: Pool a -> [Handle] -> IO a -> STM Handle
submitDependentTask_ p parents t = do
    child <- submitTask_ p t
    forM_ parents $ sequenceTasks p child
    return child

-- | Poll the given task, returning 'Nothing' if it hasn't started yet or is
--   currently executing, and a 'Just' value if a final result is known.
pollTaskEither :: Pool a -> Handle -> STM (Maybe (Either SomeException a))
pollTaskEither p h = do
    -- If not, see if it's a pending task.  If not, do not wait at all because
    -- it will never start!
    g <- readTVar (tasks p)
    if not (gelem h g)
        then except $ TaskUnknown h
        else do
             let tv = getTaskVar g h
             status <- readTVar tv
             case status of
                 Observed -> except $ TaskAlreadyObserved h
                 Started x -> do
                     mres <- pollSTM x
                     case mres of
                         Nothing -> return ()
                         Just _ -> do
                             -- Task handles are removed when the user has
                             -- inspected their contents.  Otherwise, they
                             -- remain in the table as zombies, just as happens
                             -- on Unix.
                             writeTVar tv Observed
                             cleanupTask p h
                     return mres
                 _ -> return Nothing
  where
    except = return . Just . Left . toException

-- | Poll the given task, as with 'pollTaskEither', but re-raise any
--   exceptions that were raised in the task's thread.
pollTask :: Pool a -> Handle -> STM (Maybe a)
pollTask p h = do
    mres <- pollTaskEither p h
    case mres of
        Just (Left e)  -> throw e
        Just (Right x) -> return $ Just x
        Nothing        -> return Nothing

-- | Wait until the given task has completed, then return its final status.
waitTaskEither :: Pool a -> Handle -> STM (Either SomeException a)
waitTaskEither p h = do
    mres <- pollTaskEither p h
    case mres of
        Nothing -> retry
        Just x  -> return x

-- | Wait until the given task is completed, but re-raise any exceptions that
--   were raised in the task's thread.
waitTask :: Pool a -> Handle -> STM a
waitTask p h = do
    mres <- waitTaskEither p h
    case mres of
        Left e  -> throw e
        Right x -> return x

-- | Execute an IO action, passing it a running pool with N available slots.
withPool :: Int -> (Pool a -> IO b) -> IO b
withPool n f = do
    p <- createPool n
    withAsync (runPool p) $ const $ f p

-- | Helper function used by several of the variants of 'mapTasks' below.
mapTasksWorker :: Traversable t
               => Pool a
               -> t (IO a)
               -> (IO (t b) -> IO (t c))
               -> (Pool a -> Handle -> STM b)
               -> IO (t c)
mapTasksWorker p fs f g = do
    hs <- forM fs $ atomically . submitTask p
    f $ forM hs $ atomically . g p

-- | Run a group of up to N tasks at a time concurrently, returning the
--   results in order.  The order of execution is random, but the results are
--   returned in order.  This version is more optimal than mapTasks in the
--   case where the Pool type is the same as the result type of each action.
mapTasks :: Traversable t => Pool a -> t (IO a) -> IO (t a)
mapTasks p fs = mapTasksWorker p fs id waitTask

-- | Run a group of up to N tasks at a time concurrently, returning the
--   results in order.  The order of execution is random, but the results are
--   returned in order.  This function does not care about the type the Pool is
--   mapped over, and so can produce values of any type 'a' from any Pool.
--   This comes at a cost of some overhead, however (an extra 'TMVar' per task
--   to communicate the result).
mapTasksAny :: Traversable t => Pool u -> t (IO a) -> IO (t a)
mapTasksAny p = runTask p . sequenceA . fmap task

-- -- | Run a group of up to N tasks at a time concurrently, returning the
-- --   results in order.  The order of execution is random, but the results are
-- --   returned in order.
mapTasksE :: Traversable t => Pool a -> t (IO a) -> IO (t (Either SomeException a))
mapTasksE p fs = mapTasksWorker p fs id waitTaskEither

-- | Run a group of up to N tasks at a time concurrently, ignoring the
--   results.
mapTasks_ :: Foldable t => Pool a -> t (IO a) -> IO ()
mapTasks_ p fs = forM_ fs $ atomically . submitTask_ p

-- | Run a group of up to N tasks at a time concurrently, ignoring the
--   results.  This version of the function does not care about the type the
--   Pool is mapped over, and so can produce values of any type 'a' from any
--   Pool.  This comes at the cost of some overhead, however.
mapTasksAny_ :: Foldable t => Pool u -> t (IO ()) -> IO ()
mapTasksAny_ p = runTask p . foldr ((*>) . task) (pure ())

-- -- | Run a group of up to N tasks at a time concurrently, ignoring the
-- --   results, but returning whether an exception occurred for each task.
mapTasksE_ :: Traversable t => Pool a -> t (IO a) -> IO (t (Maybe SomeException))
mapTasksE_ p fs = mapTasksWorker p fs (fmap (fmap leftToMaybe)) waitTaskEither
  where
    leftToMaybe :: Either a b -> Maybe a
    leftToMaybe = either Just (const Nothing)

-- | Execute a group of tasks (where only N tasks at most may run,
--   corresponding to the number of available slots in the pool), returning
--   the first result or failure.  'Nothing' is returned if no tasks were
--   provided.
mapTasksRace :: Traversable t
             => Int -> t (IO a) -> IO (Maybe (Either SomeException a))
mapTasksRace n fs = withPool n $ \p -> do
    forM_ fs $ atomically . submitTask p
    g <- atomically $ readTVar (tasks p)
    if Gr.isEmpty g
        then return Nothing
        else loopM p
  where
    loopM p = do
        ps <- atomically $ do
            g <- readTVar (tasks p)
            ps <- catMaybes <$> mapM (getTaskAsync g) (nodes g)
            check (not (null ps))
            return ps
        (_, eres) <- waitAnyCatchCancel ps
        cancelAll p
        return $ Just eres

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
          => Pool a              -- ^ Pool to execute the tasks within
          -> t (IO a)            -- ^ Set of Monoid-yielding IO actions
          -> STM Handle          -- ^ Returns a Handle to the final result task
mapReduce p fs = do
    -- Submit all the tasks right away, and jobs to combine all those results.
    -- Since we're working with a Monoid, it doesn't matter what order they
    -- complete in, or what order we combine the results in, just as long we
    -- each combination waits on the results it intends to combine.
    hs <- sequenceA $ foldMap ((:[]) <$> submitTask p) fs
    loopM hs
  where
    loopM hs = do
        hs' <- squeeze hs
        case hs' of
            []  -> error "mapReduce: impossible"
            [x] -> return x
            xs  -> loopM xs

    squeeze []  = (:[]) <$> submitTask p (return mempty)
    squeeze [x] = return [x]
    squeeze (x:y:xs) = do
        t <- submitTask p $ do
            meres <- atomically $ do
                -- These polls should by definition always succeed, since this
                -- task should not start until results are available.
                eres1 <- pollTaskEither p x
                eres2 <- pollTaskEither p y
                case liftM2 (<>) <$> eres1 <*> eres2 of
                    Nothing -> retry
                    Just a  -> return a
            case meres of
                Left e  -> throwIO e
                Right a -> return a
        forM_ [x, y] (unsafeSequenceTasks p t)
        case xs of
            [] -> return [t]
            _  -> (t :) <$> squeeze xs

-- | Execute a group of tasks concurrently (using up to N active threads,
--   depending on the pool), and feed results to the continuation immediately
--   as they become available, in whatever order they complete in.  That
--   function may return a monoid value which is accumulated to yield the
--   final result.
scatterFoldM :: (Foldable t, Monoid b)
             => Pool a -> t (IO a) -> (Either SomeException a -> IO b) -> IO b
scatterFoldM p fs f = do
    hs <- atomically $ sequenceA $ foldMap ((:[]) <$> submitTask p) fs
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
        eres <- pollTaskEither p h
        return $ case eres of
            Nothing        -> acc
            Just (Left e)  -> Just (h, Left e)
            Just (Right x) -> Just (h, Right x)

-- | The 'Tasks' Applicative and Monad allow for task dependencies to be built
--   using applicative and do notation.  Monadic evaluation is sequenced,
--   while applicative evaluation is done concurrently for each argument.
newtype Task a = Task
    { runTask' :: forall b. Pool b -> IO (IO a)
    }

runTask :: Pool b -> Task a -> IO a
runTask pool ts = join $ runTask' ts pool

task :: IO a -> Task a
task action = Task $ \_ -> return action

instance Functor Task where
    fmap f (Task k) = Task $ fmap (fmap (liftM f)) k

instance Applicative Task where
    pure x = Task $ \_ -> return (return x)
    Task f <*> Task x = Task $ \pool -> do
        xa <- x pool
        x' <- wrap (unsafeCoerce pool) xa
        fa <- f pool
        return $ fa <*> x'
      where
        wrap pool action = atomically $ do
            mv <- newEmptyTMVar
            _  <- submitTask_ pool $
                catch (atomically . putTMVar mv . Right =<< action)
                      (atomically . putTMVar mv . Left)
            return $ do
                eres <- atomically $ takeTMVar mv
                case eres of
                    Left e  -> throwIO (e :: SomeException)
                    Right y -> return y

instance Monad Task where
    return = pure
    Task m >>= f = Task $ \pool ->
        join (m pool) >>= flip runTask' pool . f

instance MonadIO Task where
    liftIO = task
