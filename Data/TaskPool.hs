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

    , waitTask
    , waitTaskEither
    , pollTask
    , pollTaskEither

    , mapTasks
    , mapTasksE
    , mapTasks_
    , mapTasksE_
    , mapTasksRace
    ) where

import Data.TaskPool.Internal
