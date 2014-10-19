-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Concurrent.Async.Pool
-- Copyright   :  (c) Simon Marlow 2012, John Wiegley 2014
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  John Wiegley <johnw@newartisans.com>
-- Stability   :  provisional
-- Portability :  non-portable (requires concurrency)
--
-- This module provides a set of operations for running IO operations
-- asynchronously and waiting for their results.  It is a thin layer over the
-- basic concurrency operations provided by "Control.Concurrent".  The main
-- additional functionality it provides is the ability to wait for the return
-- value of a thread, plus functions for managing task pools, work groups, and
-- many-to-many dependencies between tasks.  The interface also provides some
-- additional safety and robustness over using threads and @MVar@ directly.
--
-- The basic type is @'Async' a@, which represents an asynchronous @IO@ action
-- that will return a value of type @a@, or die with an exception.  An @Async@
-- corresponds to either a thread, or a @Handle@ to an action waiting to be
-- spawned.  This makes it possible to submit very large numbers of tasks,
-- with only N threads active at one time.
--
-- For example, to fetch two web pages at the same time, we could do
-- this (assuming a suitable @getURL@ function):
--
-- >    withTaskGroup 4 $ \g -> do
-- >       a1 <- async g (getURL url1)
-- >       a2 <- async g (getURL url2)
-- >       page1 <- wait a1
-- >       page2 <- wait a2
-- >       ...
--
-- where 'async' submits the operation to the worker group (and from which it
-- is spawned in a separate thread), and 'wait' waits for and returns the
-- result.  The number 4 indicates the maximum number of threads which may be
-- spawned at one time.  If the operation throws an exception, then that
-- exception is re-thrown by 'wait'.  This is one of the ways in which this
-- library provides some additional safety: it is harder to accidentally
-- forget about exceptions thrown in child threads.
--
-- A slight improvement over the previous example is this:
--
-- >    withTaskGroup 4 $ \g -> do
-- >       withAsync g (getURL url1) $ \a1 -> do
-- >       withAsync g (getURL url2) $ \a2 -> do
-- >       page1 <- wait a1
-- >       page2 <- wait a2
-- >       ...
--
-- 'withAsync' is like 'async', except that the 'Async' is automatically
-- killed (or unscheduled, using 'cancel') if the enclosing IO operation
-- returns before it has completed.  Consider the case when the first 'wait'
-- throws an exception; then the second 'Async' will be automatically killed
-- rather than being left to run in the background, possibly indefinitely.
-- This is the second way that the library provides additional safety: using
-- 'withAsync' means we can avoid accidentally leaving threads running.
-- Furthermore, 'withAsync' allows a tree of threads to be built, such that
-- children are automatically killed if their parents die for any reason.
--
-- The pattern of performing two IO actions concurrently and waiting for their
-- results is packaged up in a combinator 'concurrently', so we can further
-- shorten the above example to:
--
-- >    withTaskGroup 4 $ \g -> do
-- >       (page1, page2) <- concurrently g (getURL url1) (getURL url2)
-- >       ...
--
-- The 'Functor' instance can be used to change the result of an 'Async'.  For
-- example:
--
-- > ghci> a <- async g (return 3)
-- > ghci> wait a
-- > 3
-- > ghci> wait (fmap (+1) a)
-- > 4

module Control.Concurrent.Async.Pool
    (
    -- * Asynchronous actions
    Async,

    -- * Task pools and groups
    withTaskGroup, withTaskGroupIn,
    Pool, createPool,
    TaskGroup, createTaskGroup, runTaskGroup,

    -- ** Spawning tasks
    async, asyncBound, asyncOn, asyncWithUnmask, asyncOnWithUnmask,
    asyncSTM,

    -- ** Dependent tasks
    taskHandle, asyncAfter, asyncAfterAll,
    makeDependent, unsafeMakeDependent,

    -- ** Spawning with automatic 'cancel'ation
    withAsync, withAsyncBound, withAsyncOn, withAsyncWithUnmask,
    withAsyncOnWithUnmask,

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

    -- ** Lists of actions
    mapTasks, mapTasks_, mapTasksE, mapTasksE_,
    mapRace, mapReduce,
    scatterFoldMapM,

    -- ** The Task Monad and Applicative
    Task, runTask, task,

    -- * Other utilities
    race, race_,
    concurrently, mapConcurrently, Concurrently(..)
    ) where

import Control.Concurrent.Async.Pool.Async
import Control.Concurrent.Async.Pool.Internal
