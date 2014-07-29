{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad
import Data.TaskPool
import Test.Hspec

main :: IO ()
main = do
    p <- createPool 4
    link =<< async (runPool p)

    sync <- newMVar ()
    hs <- forM [(1 :: Int) .. 30] $ \h -> atomically $ submitTask p $ do
        threadDelay ((h `mod` 4) * 100000)
        modifyMVar_ sync $ const $ putStrLn $ "Task " ++ show h

    forM_ hs $ atomically . waitTask p
    putStrLn "All done"
