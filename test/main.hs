{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeSynonymInstances #-}

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad
import           Data.Functor
import           Data.Graph.Inductive.Graph as Gr
import qualified Data.IntMap as M
import           Data.Monoid
import           Data.TaskPool.Internal
import           Data.Time
import           Test.Hspec

instance Show (TaskVar a) where
    show _ = "Task"

testAvail p x = do
    a <- atomically $ readTVar (avail p)
    a `shouldBe` x

testGraph p f x = do
    g <- atomically $ readTVar (tasks p)
    (f g `shouldBe` x) `onException` prettyPrint g

graphPict p x = do
    g <- atomically $ readTVar (tasks p)
    prettify g `shouldBe` x

testProcs p f x = do
    ps <- atomically $ do
        g <- readTVar (tasks p)
        foldM (go g) M.empty (nodes g)
    (f ps `shouldBe` x) `onException` print (M.keys ps)
  where
    go g acc h' = do
        mres <- getTaskAsync g h'
        return $ case mres of
            Nothing -> acc
            Just x  -> M.insert h' x acc

main :: IO ()
main = hspec $ do
  describe "simple tasks" $ do
    it "completes a task" $ do
        p <- createPool 8

        -- Upon creation of the pool, both the task graph and the process map
        -- are empty.
        testAvail p 8
        testGraph p isEmpty True
        testProcs p M.null True

        -- We submit a task, so that the graph has an entry, but the process
        -- map is still empty.
        h <- atomically $ submitTask p $ return (42 :: Int)
        testGraph p isEmpty False
        testProcs p M.null True

        -- Start running the pool in another thread and wait 100ms.  This is
        -- time enough for the task to finish.
        a <- async (runPool p)
        threadDelay 100000

        -- Now the task graph should be empty, but the process map should have
        -- our completed Async in it, awaiting us to obtain the result.
        testAvail p 8
        testProcs p M.null False

        -- Wait on the task and see the result value from the task.
        res <- atomically $ waitTask p h
        res `shouldBe` 42

        -- Now the task graph should be empty, since observing the final state
        -- removed the process entry from the map.
        testGraph p isEmpty True
        testProcs p M.null True

        -- Cancel the thread that was running the pool.
        cancel a

    it "completes two concurrent tasks" $ do
        p <- createPool 8

        testAvail p 8
        testGraph p isEmpty True
        testProcs p M.null True

        h1 <- atomically $ submitTask p $ return (42 :: Int)
        h2 <- atomically $ submitTask p $ return 43

        testGraph p isEmpty False
        testProcs p M.null True

        graphPict p "0:Task->[]\n1:Task->[]\n"

        a <- async (runPool p)
        threadDelay 100000

        testAvail p 8
        testProcs p M.size 2

        res <- atomically $ waitTask p h1
        res `shouldBe` 42
        res' <- atomically $ waitTask p h2
        res' `shouldBe` 43

        testGraph p isEmpty True
        testProcs p M.null True

        cancel a

    it "completes two linked tasks" $ do
        p <- createPool 8

        testAvail p 8
        testGraph p isEmpty True
        testProcs p M.null True

        -- Start two interdependent tasks.  The first task waits a bit and
        -- then writes a value into a TVar.  The second task does not wait, but
        -- immediately reads the value from the TVar and adds to it.
        -- Sequencing should cause these two to happen in series.
        x <- atomically $ newTVar (0 :: Int)
        h1 <- atomically $ submitTask p $ do
            threadDelay 50000
            atomically $ writeTVar x 42
            return 42
        h2 <- atomically $ submitDependentTask p [h1] $ do
            y <- atomically $ readTVar x
            return $ y + 100

        testGraph p isEmpty False
        testProcs p M.null True

        graphPict p "0:Task->[(Pending,1)]\n1:Task->[]\n"

        a <- async (runPool p)
        threadDelay 250000

        testAvail p 8
        testProcs p M.size 2

        res <- atomically $ waitTask p h1
        res `shouldBe` 42
        res' <- atomically $ waitTask p h2
        res' `shouldBe` 142

        testGraph p isEmpty True
        testProcs p M.null True

        cancel a

  describe "map reduce" $ do
    it "sums a group of integers" $ do
        p <- createPool 8 :: IO (Pool (Sum Int))
        h <- atomically $ mapReduce p $ map (return . Sum) [1..10]
        g <- atomically $ readTVar (tasks p)
        withAsync (runPool p) $ const $ do
            eres <- atomically $ waitTaskEither p h
            case eres of
                Left e  -> throwIO e
                Right x -> x `shouldBe` Sum 55

  describe "scatter fold" $ do
      it "sums in random order" $ withPool 8 $ \p -> do
        let go x = do
                threadDelay (10000 * (x `mod` 3))
                return $ Sum x
        res <- scatterFoldM p (map go [1..20]) $ \ex ->
            case ex of
                Left e  -> mempty <$ print ("Hmmm... " ++ show e)
                Right x -> return x
        getSum res `shouldBe` 210

  describe "applicative style" $ do
      it "maps tasks" $ withPool 8 $ \p -> do
          start <- getCurrentTime
          x <- mapTasks p (replicate 8 (threadDelay 1000000 >> return (1 :: Int)))
          sum x `shouldBe` 8
          end <- getCurrentTime
          let diff = diffUTCTime end start
          diff < 1.2 `shouldBe` True

      it "counts to ten in one second" $ withPool 8 $ \p -> do
          start <- getCurrentTime
          x <- runTask p $
              let k a b c d e f g h = a + b + c + d + e + f + g + h
                  h = task (threadDelay 1000000 >> return (1 :: Int))
              in k <$> h <*> h <*> h <*> h <*> h <*> h <*> h <*> h
          x `shouldBe` 8
          end <- getCurrentTime
          let diff = diffUTCTime end start
          diff < 1.2 `shouldBe` True
