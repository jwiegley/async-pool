{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}

module TaskPool
    ( createPool
    , setPoolSlots
    , cancelAll
    , sequenceTasks
    , submitTask
    , submitDependentTask
    , cancelTask
    , waitTask
    , waitTaskEither
    , pollTask
    , pollTaskEither
    , main
    ) where

import           Control.Applicative hiding (empty)
import           Control.Concurrent (threadDelay, newMVar, modifyMVar_)
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Lens
import           Control.Monad (when)
import           Data.Foldable
import           Data.Graph.Inductive.Graph as Gr hiding ((&))
import           Data.Graph.Inductive.PatriciaTree
import           Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap
import           Data.Maybe (mapMaybe)
import           Data.Monoid
import           Data.Traversable
import           Prelude hiding (mapM_, mapM, foldr, all, concatMap)

type Handle = Node
type Task a = IO a

data Status = Pending | Completed deriving (Eq, Show)

type TaskGraph a = Gr (Task a) Status

data Pool a = Pool
    { _slots  :: TVar Int
    , _avail  :: TVar Int
    , _procs  :: TVar (IntMap (Async a))
    , _tasks  :: TVar (TaskGraph a)
      -- ^ A task graph represents a partially ordered set P with subset
      --   S such that for every x ∈ S and y ∈ P, either x ≤ y or x is
      --   unrelated to y.  S becomes the set of all tasks which may
      --   execute concurrently.
      --
      --   We use a graph representation to make determination of S
      --   efficient, and to record termination of parents in the
      --   dependency structure.
    , _tokens :: TVar Int
    }

makeClassy ''Pool

type TaskInfo a = (Handle, Task a)

-- | Return the list of unlabeled nodes which are ready for execution.
--   This decreases the number of available slots, but does not remove the
--   nodes from the graph.
getReadyNodes :: Pool a -> TaskGraph a -> STM [Node]
getReadyNodes p g = do
    availSlots <- readTVar (p^.avail)
    ps <- readTVar (p^.procs)
    let readyNodes = take availSlots
                   $ filter (\n -> isReady n && IntMap.notMember n ps)
                   $ nodes g
    modifyTVar (p^.avail) (\x -> x - length readyNodes)
    return readyNodes
  where
    -- | Returns True for every node in the graph which has either no
    --   dependencies, or no incomplete dependencies.
    isReady x = all (^._3.to (== Completed)) (out g x)

-- | Given a task handle, return everything we need to know about that
--   task.
getTaskInfo :: TaskGraph a -> Handle -> TaskInfo a
getTaskInfo g h = let (_toNode, _, t, _fromNode) = context g h in (h, t)

-- | Return information about the list of tasks ready to execute,
--   sufficient both to start them and to remove them from the graph
--   afterward.
getReadyTasks :: Pool a -> STM [TaskInfo a]
getReadyTasks p = do
    g <- readTVar (p^.tasks)
    map (getTaskInfo g) <$> getReadyNodes p g

-- | Begin executing tasks within the given pool.  The number of slots
--   determine how many threads can execute concurrently.  This number
--   can be adjusted dynamically, although reducing it does not cause any
--   threads to stop.
--
--   Note: Setting the number of available slots to zero has the effect
--   of exiting this function, so that afterward runPool will need to be
--   called again.  This is done by setPoolSlots.
runPool :: Pool a -> IO ()
runPool p = do
    cnt <- atomically $ readTVar (p^.slots)
    when (cnt > 0) $ do
        ready <- atomically $ getReadyTasks p
        xs <- ready ^!! traverse.act
            (\ti -> (,) <$> pure ti <*> startTask p ti)
        atomically $ modifyTVar (p^.procs) $ \ms ->
            foldl' (\m ((h, _), x) -> IntMap.insert h x m) ms xs
        runPool p

-- | Start a task within the given pool.  This begins execution as soon
--   as the runtime is able.
startTask :: Pool a -> TaskInfo a -> IO (Async a)
startTask p (h, action) = async $ finally action $ atomically $ do
    ss <- readTVar (p^.slots)
    modifyTVar (p^.avail) $ \a -> min (succ a) ss

    -- Once the task is done executing, we must alter the graph so any
    -- dependent children will know their parent has completed.
    modifyTVar (p^.tasks) $ \g ->
        case zip (repeat h) (Gr.pre g h) of
            -- If nothing dependend on this task, prune it from the
            -- graph, as well as any parents which now have no
            -- dependents.  Otherwise, mark the edges as Completed so
            -- dependent children can execute.
            [] -> dropTask h g
            es -> insEdges (completeEdges es) $ delEdges es g
  where
    completeEdges = map (\(f, t) -> (f, t, Completed))

    dropTask k gr = foldl' f (delNode k gr) (Gr.suc gr k)
      where
        f g n = if indeg g n == 0 then dropTask n g else g

-- | Create a thread pool for executing multiple, potentionally
--   inter-dependent tasks concurrently.
createPool :: Int                -- ^ Maximum number of running tasks.
           -> IO (Async (), Pool a)
createPool cnt = do
    p <- atomically $
        Pool <$> newTVar cnt
             <*> newTVar cnt
             <*> newTVar mempty
             <*> newTVar Gr.empty
             <*> newTVar 0
    a <- async $ runPool p
    return (a, p)

setPoolSlots :: Pool a -> Int -> STM ()
setPoolSlots p n = do
    ss <- readTVar (p^.slots)
    let diff = n - ss
    modifyTVar (p^.avail) (\x -> max 0 (x + diff))
    writeTVar (p^.slots) (max 0 n)

cancelAll :: Pool a -> IO ()
cancelAll p = (mapM_ cancel =<<) $ atomically $ do
    writeTVar (p^.tasks) Gr.empty
    xs <- IntMap.elems <$> readTVar (p^.procs)
    writeTVar (p^.procs) mempty
    return xs

cancelTask :: Pool a -> Handle -> IO ()
cancelTask p h = (mapM_ cancel =<<) $ atomically $ do
    g <- readTVar (p^.tasks)
    hs <- if gelem h g
          then do
              let xs = nodeList g h
              modifyTVar (p^.tasks) $ \g' ->
                  foldl' (flip delNode) g' xs
              return xs
          else return []
    ps <- readTVar (p^.procs)
    let ts = mapMaybe (`IntMap.lookup` ps) hs
    writeTVar (p^.procs) (foldl' (flip IntMap.delete) ps hs)
    return ts
  where
    nodeList :: TaskGraph a -> Node -> [Node]
    nodeList g k = k : concatMap (nodeList g) (Gr.pre g k)

nextIdent :: Pool a -> STM Int
nextIdent p = do
    tok <- readTVar (p^.tokens)
    writeTVar (p^.tokens) (succ tok)
    return tok

submitTask :: Pool a -> Task a -> STM Handle
submitTask p action = do
    h <- nextIdent p
    modifyTVar (p^.tasks) (insNode (h, action))
    return h

-- | Given parent and child task handles, link them so that the child
--   cannot execute until the parent has finished.
sequenceTasks :: Pool a
              -> Handle          -- ^ Task to depend on (parent)
              -> Handle          -- ^ Task that depends (child)
              -> STM ()
sequenceTasks p parent child = do
    g <- readTVar (p^.tasks)
    -- If the parent is no longer in the graph, there is no need to
    -- establish a dependency.  The child can begin executing in the
    -- next free slot.
    when (gelem parent g) $
        modifyTVar (p^.tasks) (insEdge (child, parent, Pending))

submitDependentTask :: Pool a -> Task a -> Handle -> STM Handle
submitDependentTask p t parent = do
    child <- submitTask p t
    sequenceTasks p parent child
    return child

removeTaskHandle :: Pool a -> Handle -> STM ()
removeTaskHandle p = modifyTVar (p^.procs) . IntMap.delete

pollTaskEither :: Pool a -> Handle -> STM (Maybe (Either SomeException a))
pollTaskEither p h = do
    ps <- readTVar (p^.procs)
    case IntMap.lookup h ps of
        Just t  -> do
            -- First check if this is a currently executing task
            mres <- pollSTM t
            case mres of
                -- Task handles are removed when the user has inspected
                -- their contents.  Otherwise, they remain in the table
                -- as zombies, just as happens on Unix.
                Just _  -> removeTaskHandle p h
                Nothing -> return ()
            return mres

        Nothing -> do
            -- If not, see if it's a pending task.  If not, do not wait at
            -- all because it will never start!
            g <- readTVar (p^.tasks)
            return $ if gelem h g
                     then Nothing
                     else Just $ Left $ toException $
                          userError $ "Task " ++ show h ++ " unknown"

pollTask :: Pool a -> Handle -> STM (Maybe a)
pollTask p h = do
    mres <- pollTaskEither p h
    case mres of
        Just (Left e)  -> throw e
        Just (Right x) -> return $ Just x
        Nothing        -> return Nothing

waitTaskEither :: Pool a -> Handle -> STM (Either SomeException a)
waitTaskEither p h = do
    mres <- pollTaskEither p h
    case mres of
        Nothing -> retry
        Just x  -> return x

waitTask :: Pool a -> Handle -> STM a
waitTask p h = do
    mres <- waitTaskEither p h
    case mres of
        Left e  -> throw e
        Right x -> return x

main :: IO ()
main = do
    (a, p) <- createPool 4
    link a

    sync <- newMVar ()
    hs <- forM [(1 :: Int) .. 30] $ \h -> atomically $ submitTask p $ do
        threadDelay ((h `mod` 4) * 100000)
        modifyMVar_ sync $ const $ putStrLn $ "Task " ++ show h

    forM_ hs $ atomically . waitTask p
    putStrLn "All done"
