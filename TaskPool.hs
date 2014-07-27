{-# LANGUAGE TemplateHaskell #-}

module TaskPool
    ( createPool
    , setPoolSlots
    , cancelAll
    , sequenceTasks
    , submitTask
    , submitDependentTask
    , cancelTask
    , waitOnTask
    , waitOnTaskEither
    , pollTask
    , pollTaskEither
    ) where

import           Control.Applicative hiding (empty)
import           Control.Concurrent (threadDelay)
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
import           Data.Monoid
import           Data.Traversable
import           Prelude hiding (mapM_, mapM, foldr)

type Task   = IO ()
type Handle = Node

data Pool = Pool
    { _slots  :: TVar Int
    , _avail  :: TVar Int
    , _procs  :: TVar (IntMap (Async ()))
    , _tasks  :: TVar (Gr Task ())
    , _tokens :: TVar Int
    }

makeClassy ''Pool

data TaskInfo = TaskInfo
    { _taskHandle :: Handle
    , _task       :: Task
    , _taskEdges  :: [(Node, Node)]
    }

makeClassy ''TaskInfo

-- | Return the list of unlabeled nodes which are ready for execution.
--   This decreases the number of available slots, but does not remove the
--   nodes from the graph.
getReadyNodes :: Pool -> Gr Task () -> STM [Node]
getReadyNodes p g = do
    avl <- readTVar (p^.avail)
    let ready = take avl $ filter (\x -> outdeg g x == 0) $ nodes g
    modifyTVar (p^.avail) (\x -> x - length ready)
    return ready

getTaskInfo :: Gr Task () -> Handle -> TaskInfo
getTaskInfo g h =
    let (toNode, _, t, fromNode) = context g h
    in TaskInfo
        { _taskHandle = h
        , _task       = t
        , _taskEdges  = zip (map snd toNode) (repeat h) ++
                        zip (repeat h) (map snd fromNode)
        }

-- | Return information about the list of tasks ready to execute,
--   sufficient both to start them and to remove them from the graph
--   afterward.
getReadyTasks :: Pool -> STM [TaskInfo]
getReadyTasks p = do
    g <- readTVar (p^.tasks)
    map (getTaskInfo g) <$> getReadyNodes p g

-- | Drop the given list of tasks from the graph.  They should be in the
--   process map before this function is called.
dropNodes :: Pool -> [TaskInfo] -> STM ()
dropNodes p ns = modifyTVar (p^.tasks) $ \ts' -> foldl' (flip go) ts' ns
  where
    go ti = delNode (ti^.taskHandle) . delEdges (ti^.taskEdges)

runPool :: Pool -> IO ()
runPool p = do
    cnt <- atomically $ readTVar (p^.slots)
    when (cnt > 0) $ do
        ready <- atomically $ getReadyTasks p
        xs <- ready ^!! traverse.act
            (\ti -> (,) <$> pure ti <*> startTask p ti)
        atomically $ do
            modifyTVar (p^.procs) $ \ms ->
                foldl' (\m (ti, x) ->
                             IntMap.insert (ti^.taskHandle) x m) ms xs
            dropNodes p (map fst xs)
        runPool p

startTask :: Pool -> TaskInfo -> IO (Async ())
startTask p ti = async $ do
    res <- ti^.task
    atomically $ do
        ss <- readTVar (p^.slots)
        modifyTVar (p^.avail) $ \a -> min (succ a) ss
    return res

createPool :: Int -> IO Pool
createPool cnt = do
    p <- atomically $
        Pool <$> newTVar cnt
             <*> newTVar cnt
             <*> newTVar mempty
             <*> newTVar Gr.empty
             <*> newTVar 0
    a <- async $ runPool p
    link a
    return p

setPoolSlots :: Pool -> Int -> STM ()
setPoolSlots p n = do
    ss <- readTVar (p^.slots)
    let diff = n - ss
    modifyTVar (p^.avail) (\x -> max 0 (x + diff))
    writeTVar (p^.slots) (max 0 n)

cancelAll :: Pool -> IO ()
cancelAll p = do
    xs <- atomically $ do
        writeTVar (p^.tasks) Gr.empty
        xs <- IntMap.elems <$> readTVar (p^.procs)
        writeTVar (p^.procs) mempty
        return xs
    mapM_ cancel xs

cancelTask :: Pool -> Handle -> IO ()
cancelTask p h = do
    mres <- atomically $ do
        g <- readTVar (p^.tasks)
        when (gelem h g) $ do
            let ti = getTaskInfo g h
            dropNodes p [ti]
        t <- IntMap.lookup h <$> readTVar (p^.procs)
        removeTaskHandle p h
        return t
    case mres of
        Just t  -> cancel t
        Nothing -> return ()

nextIdent :: Pool -> STM Int
nextIdent p = do
    tok <- readTVar (p^.tokens)
    writeTVar (p^.tokens) (succ tok)
    return tok

submitTask :: Pool -> Task -> STM Handle
submitTask p t = do
    h <- nextIdent p
    modifyTVar (p^.tasks) (insNode (h, t))
    return h

sequenceTasks :: Pool
              -> Handle          -- ^ Task to depend on (parent)
              -> Handle          -- ^ Task that depends (child)
              -> STM ()
sequenceTasks p parent child =
    modifyTVar (p^.tasks) (insEdge (parent, child, ()))

submitDependentTask :: Pool -> Task -> Handle -> STM Handle
submitDependentTask p t parent = do
    child <- submitTask p t
    sequenceTasks p parent child
    return child

removeTaskHandle :: Pool -> Handle -> STM ()
removeTaskHandle p = modifyTVar (p^.procs) . IntMap.delete

pollTaskEither :: Pool -> Handle -> STM (Maybe (Either SomeException ()))
pollTaskEither p h = do
    ps <- readTVar (p^.procs)
    case IntMap.lookup h ps of
        Just t  -> do
            -- First check if this is a currently executing task
            mres <- pollSTM t
            case mres of
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

pollTask :: Pool -> Handle -> STM (Maybe ())
pollTask p h = do
    mres <- pollTaskEither p h
    case mres of
        Just (Left e)  -> throw e
        Just (Right x) -> return $ Just x
        Nothing        -> return Nothing

waitOnTaskEither :: Pool -> Handle -> STM (Either SomeException ())
waitOnTaskEither p h = do
    mres <- pollTaskEither p h
    case mres of
        Nothing -> retry
        Just x  -> return x

waitOnTask :: Pool -> Handle -> STM ()
waitOnTask p h = do
    mres <- waitOnTaskEither p h
    case mres of
        Left e  -> throw e
        Right x -> return x

main :: IO ()
main = do
    p <- createPool 16
    hs <- forM [(1 :: Int) .. 30] $ \h ->
        atomically $ submitTask p $ putStrLn $ "Task " ++ show h
    _ <- forM_ [(1 :: Int) .. 5] $ const $ do
        putStrLn "Polling tasks..."
        mapM_ ((try :: IO a -> IO (Either SomeException a))
                   . atomically . pollTaskEither p) hs
        threadDelay 1000000
    putStrLn "All done"
