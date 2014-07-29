module Data.TaskPool
    ( Pool
    , Handle

    , createPool
    , runPool
    , setPoolSlots
    , cancelAll

    , submitTask
    , submitDependentTask
    , cancelTask
    , sequenceTasks

    , waitTask
    , waitTaskEither
    , pollTask
    , pollTaskEither
    ) where

import Data.TaskPool.Internal
