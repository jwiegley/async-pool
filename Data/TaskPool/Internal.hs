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
import           Data.Maybe (mapMaybe)
import           Data.Monoid
import           Data.Traversable
import           Prelude hiding (mapM_, mapM, foldr, all, any, concatMap,
                                 foldl1)

-- | A 'Handle' is a unique reference to a task that has submitted to a
--   'Pool'.
type Handle      = Node
type TaskInfo a  = (Handle, Task a)
type TaskGraph a = Gr (Task a) Status
type Task a      = IO a

data Status = Pending | Completed deriving (Eq, Show)

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
    { slots  :: TVar Int
      -- ^ The total number of execution slots in the pool.  If nothing is
      --   running, this is also the number of available slots.  This can be
      --   changed dynamically using 'setPoolSlots'.
    , avail  :: TVar Int
      -- ^ The number of available execution slots in the pool.
    , procs  :: TVar (IntMap (Async a))
      -- ^ The active or completed process table.  For every task running in a
      --   thread, this holds the Async value governing that thread; for every
      --   completed task, it holds the Async value that records its
      --   completion value or exception status.  These entries are inserted
      --   whenever a thread is started, and are cleared by ultimately calling
      --   'pollTaskEither' (which all the other polling and waiting functions
      --   also call).
      --
      --   Note that submitting a task with 'submitTask_' or
      --   'submitDependentTask_' will remove the thread's Async value
      --   immediately at the end of the task, causing it to be garbage
      --   collected.
    , tasks  :: TVar (TaskGraph a)
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
getReadyNodes :: Pool a -> TaskGraph a -> STM [Node]
getReadyNodes p g = do
    availSlots <- readTVar (avail p)
    ps <- readTVar (procs p)
    let readyNodes = take availSlots
                   $ filter (\n -> isReady n && IntMap.notMember n ps)
                   $ nodes g
    modifyTVar (avail p) (\x -> x - length readyNodes)
    return readyNodes
  where
    -- | Returns True for every node for which there are no dependencies or
    --   incomplete dependencies, and which is not itself a completed
    --   dependency.  The reason for the latter condition is that we keep
    --   completed nodes with dependents in graph until their dependents have
    --   completed (recursively), so that the dependent knows only to begin
    --   when its parent has truly finished -- a fact which cannot be
    --   determined using only the process map.
    isReady x = all isCompleted (inn g x) && not (any isCompleted (out g x))

    isCompleted (_,_,Completed) = True
    isCompleted (_,_,_)         = False

-- | Given a task handle, return everything we know about that task.
getTaskInfo :: TaskGraph a -> Handle -> TaskInfo a
getTaskInfo g h = let (_toNode, _, t, _fromNode) = context g h in (h, t)

-- | Return information about the list of tasks ready to execute, sufficient
--   to start them and remove them from the graph afterwards.
getReadyTasks :: Pool a -> STM [TaskInfo a]
getReadyTasks p = do
    g <- readTVar (tasks p)
    map (getTaskInfo g) <$> getReadyNodes p g

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
        return ready
    xs <- forM ready $ \ti -> (,) <$> pure ti <*> startTask p ti
    atomically $ modifyTVar (procs p) $ \ms ->
        foldl' (\m ((h, _), x) -> IntMap.insert h x m) ms xs

-- | Start a task within the given pool.  This begins execution as soon as the
--   runtime is able to.
startTask :: Pool a -> TaskInfo a -> IO (Async a)
startTask p (h, go) = async $ finally go $ atomically $ do
    ss <- readTVar (slots p)
    modifyTVar (avail p) $ \a -> min (succ a) ss

    -- Once the task is done executing, we must alter the graph so any
    -- dependent children will know their parent has completed.
    modifyTVar (tasks p) $ \g ->
        case zip (repeat h) (Gr.suc g h) of
            -- If nothing dependend on this task, prune it from the graph, as
            -- well as any parents which now have no dependents.  Otherwise,
            -- mark the edges as Completed so dependent children can execute.
            [] -> dropTask h g
            es -> insEdges (completeEdges es) $ delEdges es g
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
    writeTVar (tasks p) Gr.empty
    xs <- IntMap.elems <$> readTVar (procs p)
    writeTVar (procs p) mempty
    return xs

-- | Cancel a task submitted to the pool.  This will unschedule it if it had not
--   begun yet, or cancel its thread if it had.
cancelTask :: Pool a -> Handle -> IO ()
cancelTask p h = (mapM_ cancel =<<) $ atomically $ do
    g <- readTVar (tasks p)
    hs <- if gelem h g
          then do
              let xs = nodeList g h
              modifyTVar (tasks p) $ \g' -> foldl' (flip delNode) g' xs
              return xs
          else return []
    ps <- readTVar (procs p)
    let ts = mapMaybe (`IntMap.lookup` ps) hs
    writeTVar (procs p) (foldl' (flip IntMap.delete) ps hs)
    return ts
  where
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
    modifyTVar (tasks p) (insNode (h, action))
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
            res <$ modifyTVar (procs p) (IntMap.delete h)

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
submitDependentTask_ :: Pool a -> Handle -> IO a -> STM Handle
submitDependentTask_ p parent t = do
    child <- submitTask_ p t
    sequenceTasks p child parent
    return child

-- | Poll the given task, returning 'Nothing' if it hasn't started yet or is
--   currently executing, and a 'Just' value if a final result is known.
pollTaskEither :: Pool a -> Handle -> STM (Maybe (Either SomeException a))
pollTaskEither p h = do
    ps <- readTVar (procs p)
    case IntMap.lookup h ps of
        Just t  -> do
            -- First check if this is a currently executing task
            mres <- pollSTM t
            case mres of
                -- Task handles are removed when the user has inspected their
                -- contents.  Otherwise, they remain in the table as zombies,
                -- just as happens on Unix.
                Just _  -> modifyTVar (procs p) (IntMap.delete h)
                Nothing -> return ()
            return mres

        Nothing -> do
            -- If not, see if it's a pending task.  If not, do not wait at all
            -- because it will never start!
            g <- readTVar (tasks p)
            return $ if gelem h g
                     then Nothing
                     else Just $ Left $ toException $
                          userError $ "Task " ++ show h ++ " unknown"

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

-- | Run a group of up to N tasks at a time concurrently, returning the
--   results in order.  The order of execution is random, but the results are
--   returned in order.
mapTasks' :: Traversable t
          => Int
          -> t (IO a)
          -> (IO (t b) -> IO (t c))
          -> (Pool a -> Handle -> STM b)
          -> IO (t c)
mapTasks' n fs f g = withPool n $ \p -> do
    hs <- forM fs $ atomically . submitTask p
    f $ forM hs $ atomically . g p

-- | Run a group of up to N tasks at a time concurrently, returning the
--   results in order.  The order of execution is random, but the results are
--   returned in order.
mapTasks :: Traversable t => Int -> t (IO a) -> IO (t a)
mapTasks n fs = mapTasks' n fs id waitTask

-- | Run a group of up to N tasks at a time concurrently, returning the
--   results in order.  The order of execution is random, but the results are
--   returned in order.
mapTasksE :: Traversable t => Int -> t (IO a) -> IO (t (Either SomeException a))
mapTasksE n fs = mapTasks' n fs id waitTaskEither

-- | Run a group of up to N tasks at a time concurrently, ignoring the
--   results.
mapTasks_ :: Foldable t => Int -> t (IO a) -> IO ()
mapTasks_ n fs = withPool n $ \p -> forM_ fs $ atomically . submitTask_ p

-- | Run a group of up to N tasks at a time concurrently, ignoring the
--   results, but returning whether an exception occurred for each task.
mapTasksE_ :: Traversable t => Int -> t (IO a) -> IO (t (Maybe SomeException))
mapTasksE_ n fs = mapTasks' n fs (fmap (fmap leftToMaybe)) waitTaskEither
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
            ps <- readTVar (procs p)
            check (not (IntMap.null ps))
            return ps
        let as = IntMap.assocs ps
        (_, eres) <- waitAnyCatchCancel (map snd as)
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
            []  -> error "Egads, impossible!"
            [x] -> return x
            xs  -> loopM xs

    squeeze []  = (:[]) <$> submitTask p (return mempty)
    squeeze [x] = return [x]
    squeeze (x:y:xs) = do
        t <- submitTask p $ do
            meres <- atomically $ do
                -- These polls should by definition always succeed, since this
                -- task should not even start until the results are available.
                eres1 <- pollTaskEither p x
                eres2 <- pollTaskEither p y
                case liftM2 (<>) <$> eres1 <*> eres2 of
                    Nothing -> retry
                    Just a  -> return a
            case meres of
                Left e  -> throwIO e
                Right a -> return a
        unsafeSequenceTasks p t x
        unsafeSequenceTasks p t y
        case xs of
            [] -> return [t]
            _  -> (t :) <$> squeeze xs

newtype Tasks a = Tasks { runTasks' :: Pool () -> IO ([Handle], IO a) }

runTasks :: Pool () -> Tasks a -> IO a
runTasks pool ts = join $ snd <$> runTasks' ts pool

task :: IO a -> Tasks a
task action = Tasks $ \_ -> return ([], action)

instance Functor Tasks where
    fmap f (Tasks k) = Tasks $ fmap (fmap (fmap (liftM f))) k

instance Applicative Tasks where
    pure x = Tasks $ \_ -> return ([], return x)
    Tasks f <*> Tasks x = Tasks $ \pool -> do
        (xh, xa) <- x pool
        (tx, x') <- wrap pool xh xa
        (fh, fa) <- f pool
        (tf, f') <- wrap pool (tx:fh) (fa <*> atomically x')
        return ([tf], atomically f')
      where
        wrap pool hs action = atomically $ do
            mv <- newEmptyTMVar
            t <- submitTask_ pool $ do
                x' <- action
                atomically $ putTMVar mv x'
                return mempty
            forM_ hs $ unsafeSequenceTasks pool t
            return (t, takeTMVar mv)

instance Monad Tasks where
    return = pure
    Tasks m >>= f = Tasks $ \pool -> do
        (_mh, mx) <- m pool
        mx >>= flip runTasks' pool . f

instance MonadIO Tasks where
    liftIO = task
