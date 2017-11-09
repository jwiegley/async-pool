{-# LANGUAGE CPP, MagicHash, UnboxedTuples, RankNTypes, GADTs #-}
#if __GLASGOW_HASKELL__ >= 701
{-# LANGUAGE Trustworthy #-}
#endif
{-# OPTIONS -Wall #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Concurrent.Async
-- Copyright   :  (c) Simon Marlow 2012
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Simon Marlow <marlowsd@gmail.com>
-- Stability   :  provisional
-- Portability :  non-portable (requires concurrency)
--
-- This module provides a set of operations for running IO operations
-- asynchronously and waiting for their results.  It is a thin layer
-- over the basic concurrency operations provided by
-- "Control.Concurrent".  The main additional functionality it
-- provides is the ability to wait for the return value of a thread,
-- but the interface also provides some additional safety and
-- robustness over using threads and @MVar@ directly.
--
-- The basic type is @'Async' a@, which represents an asynchronous
-- @IO@ action that will return a value of type @a@, or die with an
-- exception.  An @Async@ corresponds to a thread, and its 'ThreadId'
-- can be obtained with 'asyncThreadId', although that should rarely
-- be necessary.
--
-- For example, to fetch two web pages at the same time, we could do
-- this (assuming a suitable @getURL@ function):
--
-- >    do a1 <- async (getURL url1)
-- >       a2 <- async (getURL url2)
-- >       page1 <- wait a1
-- >       page2 <- wait a2
-- >       ...
--
-- where 'async' starts the operation in a separate thread, and
-- 'wait' waits for and returns the result.  If the operation
-- throws an exception, then that exception is re-thrown by
-- 'wait'.  This is one of the ways in which this library
-- provides some additional safety: it is harder to accidentally
-- forget about exceptions thrown in child threads.
--
-- A slight improvement over the previous example is this:
--
-- >       withAsync (getURL url1) $ \a1 -> do
-- >       withAsync (getURL url2) $ \a2 -> do
-- >       page1 <- wait a1
-- >       page2 <- wait a2
-- >       ...
--
-- 'withAsync' is like 'async', except that the 'Async' is
-- automatically killed (using 'cancel') if the enclosing IO operation
-- returns before it has completed.  Consider the case when the first
-- 'wait' throws an exception; then the second 'Async' will be
-- automatically killed rather than being left to run in the
-- background, possibly indefinitely.  This is the second way that the
-- library provides additional safety: using 'withAsync' means we can
-- avoid accidentally leaving threads running.  Furthermore,
-- 'withAsync' allows a tree of threads to be built, such that
-- children are automatically killed if their parents die for any
-- reason.
--
-- The pattern of performing two IO actions concurrently and waiting
-- for their results is packaged up in a combinator 'concurrently', so
-- we can further shorten the above example to:
--
-- >       (page1, page2) <- concurrently (getURL url1) (getURL url2)
-- >       ...
--
-- The 'Functor' instance can be used to change the result of an
-- 'Async'.  For example:
--
-- > ghci> a <- async (return 3)
-- > ghci> wait a
-- > 3
-- > ghci> wait (fmap (+1) a)
-- > 4

-----------------------------------------------------------------------------

module Control.Concurrent.Async.Pool.Async
    ( module Control.Concurrent.Async.Pool.Async
    , module Gr
    ) where

import Control.Concurrent.STM
import Control.Exception
import Control.Concurrent
import Control.Applicative
import Control.Monad hiding (forM, forM_, mapM, mapM_)
import Data.Foldable
import Data.Graph.Inductive.Graph as Gr hiding ((&))
import Data.Graph.Inductive.PatriciaTree as Gr
import Data.Graph.Inductive.Query.BFS as Gr
import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap
import Data.Traversable
import Prelude hiding (mapM_, mapM, foldr, all, any, concatMap, foldl1)

import GHC.Exts
import GHC.IO hiding (finally, onException)
import GHC.Conc

-- | A 'Handle' is a unique identifier for a task submitted to a 'Pool'.
type Handle    = Node
data State     = Ready | Starting | forall a. Started ThreadId (TMVar a)
data Status    = Pending | Completed deriving (Eq, Show)
type TaskGraph = Gr (TVar State) Status

instance Eq State where
    Ready        == Ready        = True
    Starting     == Starting     = True
    Started n1 _ == Started n2 _ = n1 == n2
    _            == _            = False

instance Show State where
    show Ready         = "Ready"
    show Starting      = "Starting"
    show (Started n _) = "Started " ++ show n

-- | A 'Pool' manages a collection of possibly interdependent tasks, such that
--   tasks await execution until the tasks they depend on have finished (and
--   tasks may depend on an arbitrary number of other tasks), while
--   independent tasks execute concurrently up to the number of available
--   resource slots in the pool.
--
--   Results from each task are available until the status of the task is
--   polled or waited on.  Further, the results are kept until that occurs, so
--   failing to ever wait will result in a memory leak.
--
--   Tasks may be cancelled, in which case all dependent tasks are
--   unscheduled.
data Pool = Pool
    { tasks :: TVar TaskGraph
      -- ^ The task graph represents a partially ordered set P with subset S
      --   such that for every x ∈ S and y ∈ P, either x ≤ y or x is unrelated
      --   to y.  Stated more simply, S is the set of least elements of all
      --   maximal chains in P.  In our case, ≤ relates two uncompleted tasks
      --   by dependency.  Therefore, S is equal to the set of tasks which may
      --   execute concurrently, as none of them have incomplete dependencies.
      --
      --   We use a graph representation to make determination of S more
      --   efficient (where S is just the set of roots in P expressed as a
      --   graph).  Completion status is recorded on the edges, and nodes are
      --   removed from the graph once no other incomplete node depends on
      --   them.
    , tokens :: TVar Int
      -- ^ Tokens identify tasks, and are provisioned monotonically.
    }

waitTMVar :: TMVar a -> STM ()
waitTMVar tv = do
    _ <- readTMVar tv
    return ()

syncPool :: Pool -> STM ()
syncPool p = do
    g <- readTVar (tasks p)
    forM_ (labNodes g) $ \(_h, st) -> do
        x <- readTVar st
        case x of
            Started _tid v -> waitTMVar v
            _ -> retry

data TaskGroup = TaskGroup
    { pool    :: Pool
    , avail   :: TVar Int
      -- ^ The number of available execution slots in the pool.
    , pending :: forall a. TVar (IntMap (IO ThreadId, TMVar a))
      -- ^ Nodes in the task graph that are waiting to start.
    }

-- -----------------------------------------------------------------------------
-- STM Async API


-- | An asynchronous action spawned by 'async' or 'withAsync'.
-- Asynchronous actions are executed in a separate thread, and
-- operations are provided for waiting for asynchronous actions to
-- complete and obtaining their results (see e.g. 'wait').
--
data Async a = Async
    { taskGroup  :: TaskGroup
    , taskHandle :: {-# UNPACK #-} !Handle
    , _asyncWait :: STM (Either SomeException a)
    }

getTaskVar :: TaskGraph -> Handle -> TVar State
getTaskVar g h = let (_to, _, t, _from) = context g h in t

getThreadId :: TaskGraph -> Node -> STM (Maybe ThreadId)
getThreadId g h = do
    status <- readTVar (getTaskVar g h)
    case status of
        Ready       -> return Nothing
        Starting    -> retry
        Started x _ -> return $ Just x

instance Eq (Async a) where
  Async _ a _ == Async _ b _  =  a == b

instance Ord (Async a) where
  Async _ a _ `compare` Async _ b _  =  a `compare` b

instance Functor Async where
  fmap f (Async p a w) = Async p a (fmap (fmap f) w)


-- | Spawn an asynchronous action in a separate thread.
async :: TaskGroup -> IO a -> IO (Async a)
async p = atomically . inline asyncUsing p rawForkIO

-- | Like 'async' but using 'forkOS' internally.
asyncBound :: TaskGroup -> IO a -> IO (Async a)
asyncBound p = atomically . asyncUsing p forkOS

-- | Like 'async' but using 'forkOn' internally.
asyncOn :: TaskGroup -> Int -> IO a -> IO (Async a)
asyncOn p = (atomically .) . asyncUsing p . rawForkOn

-- | Like 'async' but using 'forkIOWithUnmask' internally.
-- The child thread is passed a function that can be used to unmask asynchronous exceptions.
asyncWithUnmask :: TaskGroup -> ((forall b . IO b -> IO b) -> IO a) -> IO (Async a)
asyncWithUnmask p actionWith =
    atomically $ asyncUsing p rawForkIO (actionWith unsafeUnmask)

-- | Like 'asyncOn' but using 'forkOnWithUnmask' internally.
-- The child thread is passed a function that can be used to unmask asynchronous exceptions.
asyncOnWithUnmask :: TaskGroup -> Int -> ((forall b . IO b -> IO b) -> IO a) -> IO (Async a)
asyncOnWithUnmask p cpu actionWith =
    atomically $ asyncUsing p (rawForkOn cpu) (actionWith unsafeUnmask)

asyncUsing :: TaskGroup -> (IO () -> IO ThreadId) -> IO a -> STM (Async a)
asyncUsing p doFork action = do
    h <- nextIdent (pool p)

    var <- newEmptyTMVar
    let start = mask $ \restore ->
            doFork $ try (restore (action `finally` cleanup h))
                >>= atomically . putTMVar var

    modifyTVar (pending p) (IntMap.insert h (start, var))
    tv <- newTVar Ready
    modifyTVar (tasks (pool p)) (insNode (h, tv))

    return $ Async p h (readTMVar var)
  where
    cleanup h = atomically $ do
        modifyTVar (avail p) succ
        cleanupTask (pool p) h

-- | Return the next available thread identifier from the pool.  These are
--   monotonically increasing integers.
nextIdent :: Pool -> STM Int
nextIdent p = do
    tok <- readTVar (tokens p)
    writeTVar (tokens p) (succ tok)
    return tok

cleanupTask :: Pool -> Handle -> STM ()
cleanupTask p h =
    -- Once the task is done executing, we must alter the graph so any
    -- dependent children will know their parent has completed.
    modifyTVar (tasks p) $ \g ->
        case zip (repeat h) (Gr.suc g h) of
            -- If nothing dependend on this task and if the final result value
            -- has been observed, prune it from the graph, as well as any
            -- parents which now have no dependents.  Otherwise mark the edges
            -- as Completed so dependent children can execute.
            [] -> dropTask h g
            es -> insEdges (completeEdges es) $ delEdges es g
  where
    completeEdges = map (\(f, t) -> (f, t, Completed))

    dropTask k gr = foldl' f (delNode k gr) (Gr.pre gr k)
      where
        f g n = if outdeg g n == 0 then dropTask n g else g

-- | Spawn an asynchronous action in a separate thread, and pass its
-- @Async@ handle to the supplied function.  When the function returns
-- or throws an exception, 'cancel' is called on the @Async@.
--
-- > withAsync action inner = bracket (async action) cancel inner
--
-- This is a useful variant of 'async' that ensures an @Async@ is
-- never left running unintentionally.
--
-- Since 'cancel' may block, 'withAsync' may also block; see 'cancel'
-- for details.
--
withAsync :: TaskGroup -> IO a -> (Async a -> IO b) -> IO b
withAsync p = inline withAsyncUsing p rawForkIO

-- | Like 'withAsync' but uses 'forkOS' internally.
withAsyncBound :: TaskGroup -> IO a -> (Async a -> IO b) -> IO b
withAsyncBound p = withAsyncUsing p forkOS

-- | Like 'withAsync' but uses 'forkOn' internally.
withAsyncOn :: TaskGroup -> Int -> IO a -> (Async a -> IO b) -> IO b
withAsyncOn p = withAsyncUsing p . rawForkOn

-- | Like 'withAsync' but uses 'forkIOWithUnmask' internally.
-- The child thread is passed a function that can be used to unmask asynchronous exceptions.
withAsyncWithUnmask :: TaskGroup -> ((forall c. IO c -> IO c) -> IO a) -> (Async a -> IO b) -> IO b
withAsyncWithUnmask p actionWith =
    withAsyncUsing p rawForkIO (actionWith unsafeUnmask)

-- | Like 'withAsyncOn' but uses 'forkOnWithUnmask' internally.
-- The child thread is passed a function that can be used to unmask asynchronous exceptions
withAsyncOnWithUnmask :: TaskGroup -> Int -> ((forall c. IO c -> IO c) -> IO a) -> (Async a -> IO b) -> IO b
withAsyncOnWithUnmask p cpu actionWith = withAsyncUsing p (rawForkOn cpu) (actionWith unsafeUnmask)

withAsyncUsing :: TaskGroup -> (IO () -> IO ThreadId) -> IO a -> (Async a -> IO b)
               -> IO b
-- The bracket version works, but is slow.  We can do better by
-- hand-coding it:
withAsyncUsing p doFork = \action inner -> do
  mask $ \restore -> do
    a <- atomically $ asyncUsing p doFork $ restore action
    r <- restore (inner a) `catchAll` \e -> do cancel a; throwIO e
    cancel a
    return r

-- | Wait for an asynchronous action to complete, and return its
-- value.  If the asynchronous action threw an exception, then the
-- exception is re-thrown by 'wait'.
--
-- > wait = atomically . waitSTM
--
{-# INLINE wait #-}
wait :: Async a -> IO a
wait = atomically . waitSTM

-- | Wait for an asynchronous action to complete, and return either
-- @Left e@ if the action raised an exception @e@, or @Right a@ if it
-- returned a value @a@.
--
-- > waitCatch = atomically . waitCatchSTM
--
{-# INLINE waitCatch #-}
waitCatch :: Async a -> IO (Either SomeException a)
waitCatch = atomically . waitCatchSTM

-- | Check whether an 'Async' has completed yet.  If it has not
-- completed yet, then the result is @Nothing@, otherwise the result
-- is @Just e@ where @e@ is @Left x@ if the @Async@ raised an
-- exception @x@, or @Right a@ if it returned a value @a@.
--
-- > poll = atomically . pollSTM
--
{-# INLINE poll #-}
poll :: Async a -> IO (Maybe (Either SomeException a))
poll = atomically . pollSTM

-- | A version of 'wait' that can be used inside an STM transaction.
--
waitSTM :: Async a -> STM a
waitSTM a = do
   r <- waitCatchSTM a
   either throwSTM return r

-- | A version of 'waitCatch' that can be used inside an STM transaction.
--
{-# INLINE waitCatchSTM #-}
waitCatchSTM :: Async a -> STM (Either SomeException a)
waitCatchSTM (Async _ _ w) = w

-- | A version of 'poll' that can be used inside an STM transaction.
--
{-# INLINE pollSTM #-}
pollSTM :: Async a -> STM (Maybe (Either SomeException a))
pollSTM (Async _ _ w) = (Just <$> w) `orElse` return Nothing

-- | Cancel an asynchronous action by throwing the @ThreadKilled@
-- exception to it.  Has no effect if the 'Async' has already
-- completed.
--
-- > cancel a = throwTo (asyncThreadId a) ThreadKilled
--
-- Note that 'cancel' is synchronous in the same sense as 'throwTo'.
-- It does not return until the exception has been thrown in the
-- target thread, or the target thread has completed.  In particular,
-- if the target thread is making a foreign call, the exception will
-- not be thrown until the foreign call returns, and in this case
-- 'cancel' may block indefinitely.  An asynchronous 'cancel' can
-- of course be obtained by wrapping 'cancel' itself in 'async'.
--
{-# INLINE cancel #-}
cancel :: Async a -> IO ()
cancel = flip cancelWith ThreadKilled

-- | Cancel an asynchronous action by throwing the supplied exception
-- to it.
--
-- > cancelWith a x = throwTo (asyncThreadId a) x
--
-- The notes about the synchronous nature of 'cancel' also apply to
-- 'cancelWith'.
cancelWith' :: Exception e => Pool -> Handle -> e -> IO ()
cancelWith' p h e =
    (mapM_ (`throwTo` e) =<<) $ atomically $ do
        g <- readTVar (tasks p)
        let hs = if gelem h g then nodeList g h else []
        xs <- foldM (go g) [] hs
        writeTVar (tasks p) $ foldl' (flip delNode) g hs
        return xs
  where
    go g acc h' = maybe acc (:acc) <$> getThreadId g h'

    nodeList :: TaskGraph -> Node -> [Node]
    nodeList g k = k : concatMap (nodeList g) (Gr.suc g k)

cancelWith :: Exception e => Async a -> e -> IO ()
cancelWith (Async p h _) = cancelWith' (pool p) h

-- | Cancel an asynchronous action by throwing the @ThreadKilled@ exception to
--   it, or unregistering it from the task pool if it had not started yet.  Has
--   no effect if the 'Async' has already completed.
--
-- Note that 'cancel' is synchronous in the same sense as 'throwTo'.  It does
-- not return until the exception has been thrown in the target thread, or the
-- target thread has completed.  In particular, if the target thread is making
-- a foreign call, the exception will not be thrown until the foreign call
-- returns, and in this case 'cancel' may block indefinitely.  An asynchronous
-- 'cancel' can of course be obtained by wrapping 'cancel' itself in 'async'.
cancelAll :: TaskGroup -> IO ()
cancelAll p = do
    hs <- atomically $ do
        writeTVar (pending p) IntMap.empty
        g <- readTVar (tasks (pool p))
        return $ nodes g
    mapM_ (\h -> cancelWith' (pool p) h ThreadKilled) hs

-- | Wait for any of the supplied asynchronous operations to complete.
-- The value returned is a pair of the 'Async' that completed, and the
-- result that would be returned by 'wait' on that 'Async'.
--
-- If multiple 'Async's complete or have completed, then the value
-- returned corresponds to the first completed 'Async' in the list.
--
waitAnyCatch :: [Async a] -> IO (Async a, Either SomeException a)
waitAnyCatch asyncs =
  atomically $
    foldr orElse retry $
      map (\a -> do r <- waitCatchSTM a; return (a, r)) asyncs

-- | Like 'waitAnyCatch', but also cancels the other asynchronous
-- operations as soon as one has completed.
--
waitAnyCatchCancel :: [Async a] -> IO (Async a, Either SomeException a)
waitAnyCatchCancel asyncs =
  waitAnyCatch asyncs `finally` mapM_ cancel asyncs

-- | Wait for any of the supplied @Async@s to complete.  If the first
-- to complete throws an exception, then that exception is re-thrown
-- by 'waitAny'.
--
-- If multiple 'Async's complete or have completed, then the value
-- returned corresponds to the first completed 'Async' in the list.
--
waitAny :: [Async a] -> IO (Async a, a)
waitAny asyncs =
  atomically $
    foldr orElse retry $
      map (\a -> do r <- waitSTM a; return (a, r)) asyncs

-- | Like 'waitAny', but also cancels the other asynchronous
-- operations as soon as one has completed.
--
waitAnyCancel :: [Async a] -> IO (Async a, a)
waitAnyCancel asyncs =
  waitAny asyncs `finally` mapM_ cancel asyncs

-- | Wait for the first of two @Async@s to finish.
waitEitherCatch :: Async a -> Async b
                -> IO (Either (Either SomeException a)
                              (Either SomeException b))
waitEitherCatch left right =
  atomically $
    (Left  <$> waitCatchSTM left)
      `orElse`
    (Right <$> waitCatchSTM right)

-- | Like 'waitEitherCatch', but also 'cancel's both @Async@s before
-- returning.
--
waitEitherCatchCancel :: Async a -> Async b
                      -> IO (Either (Either SomeException a)
                                    (Either SomeException b))
waitEitherCatchCancel left right =
  waitEitherCatch left right `finally` (cancel left >> cancel right)

-- | Wait for the first of two @Async@s to finish.  If the @Async@
-- that finished first raised an exception, then the exception is
-- re-thrown by 'waitEither'.
--
waitEither :: Async a -> Async b -> IO (Either a b)
waitEither left right =
  atomically $
    (Left  <$> waitSTM left)
      `orElse`
    (Right <$> waitSTM right)

-- | Like 'waitEither', but the result is ignored.
--
waitEither_ :: Async a -> Async b -> IO ()
waitEither_ left right =
  atomically $
    (void $ waitSTM left)
      `orElse`
    (void $ waitSTM right)

-- | Like 'waitEither', but also 'cancel's both @Async@s before
-- returning.
--
waitEitherCancel :: Async a -> Async b -> IO (Either a b)
waitEitherCancel left right =
  waitEither left right `finally` (cancel left >> cancel right)

-- | Waits for both @Async@s to finish, but if either of them throws
-- an exception before they have both finished, then the exception is
-- re-thrown by 'waitBoth'.
--
waitBoth :: Async a -> Async b -> IO (a,b)
waitBoth left right =
  atomically $ do
    a <- waitSTM left
           `orElse`
         (waitSTM right >> retry)
    b <- waitSTM right
    return (a,b)


-- | Link the given @Async@ to the current thread, such that if the
-- @Async@ raises an exception, that exception will be re-thrown in
-- the current thread.
--
link :: Async a -> IO ()
link (Async _ _ w) = do
  me <- myThreadId
  void $ forkRepeat $ do
     r <- atomically $ w
     case r of
       Left e -> throwTo me e
       _ -> return ()

-- | Link two @Async@s together, such that if either raises an
-- exception, the same exception is re-thrown in the other @Async@.
--
link2 :: Async a -> Async b -> IO ()
link2 left right =
  void $ forkRepeat $ do
    r <- waitEitherCatch left right
    case r of
      Left  (Left e) -> cancelWith right e
      Right (Left e) -> cancelWith left e
      _ -> return ()


-- -----------------------------------------------------------------------------

-- | Run two @IO@ actions concurrently, and return the first to
-- finish.  The loser of the race is 'cancel'led.
--
-- > race left right =
-- >   withAsync left $ \a ->
-- >   withAsync right $ \b ->
-- >   waitEither a b
--
race :: TaskGroup -> IO a -> IO b -> IO (Either a b)

-- | Like 'race', but the result is ignored.
--
race_ :: TaskGroup -> IO a -> IO b -> IO ()

-- | Run two @IO@ actions concurrently, and return both results.  If
-- either action throws an exception at any time, then the other
-- action is 'cancel'led, and the exception is re-thrown by
-- 'concurrently'.
--
-- > concurrently left right =
-- >   withAsync left $ \a ->
-- >   withAsync right $ \b ->
-- >   waitBoth a b
concurrently :: TaskGroup -> IO a -> IO b -> IO (a,b)

#define USE_ASYNC_VERSIONS 1

#if USE_ASYNC_VERSIONS

race p left right =
  withAsync p left $ \a ->
  withAsync p right $ \b ->
  waitEither a b

race_ p left right =
  withAsync p left $ \a ->
  withAsync p right $ \b ->
  waitEither_ a b

concurrently p left right =
  withAsync p left $ \a ->
  withAsync p right $ \b ->
  waitBoth a b

#else

-- MVar versions of race/concurrently
-- More ugly than the Async versions, but quite a bit faster.

-- race :: IO a -> IO b -> IO (Either a b)
race left right = concurrently' left right collect
  where
    collect m = do
        e <- takeMVar m
        case e of
            Left ex -> throwIO ex
            Right r -> return r

-- race_ :: IO a -> IO b -> IO ()
race_ left right = void $ race left right

-- concurrently :: IO a -> IO b -> IO (a,b)
concurrently left right = concurrently' left right (collect [])
  where
    collect [Left a, Right b] _ = return (a,b)
    collect [Right b, Left a] _ = return (a,b)
    collect xs m = do
        e <- takeMVar m
        case e of
            Left ex -> throwIO ex
            Right r -> collect (r:xs) m

concurrently' :: IO a -> IO b
             -> (MVar (Either SomeException (Either a b)) -> IO r)
             -> IO r
concurrently' left right collect = do
    done <- newEmptyMVar
    mask $ \restore -> do
        lid <- forkIO $ restore (left >>= putMVar done . Right . Left)
                             `catchAll` (putMVar done . Left)
        rid <- forkIO $ restore (right >>= putMVar done . Right . Right)
                             `catchAll` (putMVar done . Left)
        let stop = killThread lid >> killThread rid
        r <- restore (collect done) `onException` stop
        stop
        return r

#endif

-- -----------------------------------------------------------------------------

-- | A value of type @Concurrently a@ is an @IO@ operation that can be
-- composed with other @Concurrently@ values, using the @Applicative@
-- and @Alternative@ instances.
--
-- Calling @runConcurrently@ on a value of type @Concurrently a@ will
-- execute the @IO@ operations it contains concurrently, before
-- delivering the result of type @a@.
--
-- For example
--
-- > (page1, page2, page3)
-- >     <- runConcurrently $ (,,)
-- >     <$> Concurrently (getURL "url1")
-- >     <*> Concurrently (getURL "url2")
-- >     <*> Concurrently (getURL "url3")
--
newtype Concurrently a = Concurrently { runConcurrently :: TaskGroup -> IO a }

instance Functor Concurrently where
  fmap f (Concurrently a) = Concurrently $ fmap f <$> a

instance Applicative Concurrently where
  pure x = Concurrently $ \_ -> return x
  Concurrently fs <*> Concurrently as =
    Concurrently $ \tg -> (\(f, a) -> f a) <$> concurrently tg (fs tg) (as tg)

instance Alternative Concurrently where
  empty = Concurrently $ \_ -> forever (threadDelay maxBound)
  Concurrently as <|> Concurrently bs =
    Concurrently $ \tg -> either id id <$> race tg (as tg) (bs tg)

-- ----------------------------------------------------------------------------

-- | Fork a thread that runs the supplied action, and if it raises an
-- exception, re-runs the action.  The thread terminates only when the
-- action runs to completion without raising an exception.
forkRepeat :: IO a -> IO ThreadId
forkRepeat action =
  mask $ \restore ->
    let go = do r <- tryAll (restore action)
                case r of
                  Left _ -> go
                  _      -> return ()
    in forkIO go

catchAll :: IO a -> (SomeException -> IO a) -> IO a
catchAll = catch

tryAll :: IO a -> IO (Either SomeException a)
tryAll = try

-- A version of forkIO that does not include the outer exception
-- handler: saves a bit of time when we will be installing our own
-- exception handler.
{-# INLINE rawForkIO #-}
rawForkIO :: IO () -> IO ThreadId
rawForkIO action = IO $ \ s ->
   case (fork# action s) of (# s1, tid #) -> (# s1, ThreadId tid #)

{-# INLINE rawForkOn #-}
rawForkOn :: Int -> IO () -> IO ThreadId
rawForkOn (I# cpu) action = IO $ \ s ->
   case (forkOn# cpu action s) of (# s1, tid #) -> (# s1, ThreadId tid #)
