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
    ) where

import Data.TaskPool.Internal
