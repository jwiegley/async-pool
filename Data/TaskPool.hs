module Data.TaskPool
    ( Pool
    , Handle

    , createPool
    , runPool
    , setPoolSlots
    , cancelAll

    , submitTask
    , submitTask_
    , submitDependentTask
    , submitDependentTask_
    , cancelTask
    , sequenceTasks
    , unsafeSequenceTasks

    , waitTask
    , waitTaskEither
    , pollTask
    , pollTaskEither

    , mapTasks
    , mapTasksE
    , mapTasks_
    , mapTasksE_
    , mapTasksRace

    , mapReduce
    , scatterFoldM

    , Tasks
    , runTasks
    , task
    ) where

import Data.TaskPool.Internal
