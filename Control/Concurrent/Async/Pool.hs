module Control.Concurrent.Async.Pool
    (

    -- * Asynchronous actions
    Async,
    -- ** Spawning
    async, asyncBound, asyncOn, asyncWithUnmask, asyncOnWithUnmask,

    -- ** Spawning with automatic 'cancel'ation
    withAsync, withAsyncBound, withAsyncOn, withAsyncWithUnmask, withAsyncOnWithUnmask,

    -- ** Quering 'Async's
    wait, poll, waitCatch, cancel, cancelWith,

    -- ** STM operations
    waitSTM, pollSTM, waitCatchSTM,

    -- ** Waiting for multiple 'Async's
    waitAny, waitAnyCatch, waitAnyCancel, waitAnyCatchCancel,
    waitEither, waitEitherCatch, waitEitherCancel, waitEitherCatchCancel,
    waitEither_,
    waitBoth,

    -- ** Linking
    link, link2,

    -- * Convenient utilities
    race, race_, concurrently, mapConcurrently,
    Concurrently(..),

    Pool, createPool, createTaskGroup, runTaskGroup, withTaskGroup,

    asyncAfter, makeDependent, taskHandle
    ) where

import Control.Concurrent.Async.Pool.Async
import Control.Concurrent.Async.Pool.Internal
