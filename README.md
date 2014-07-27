taskpool is a lightweight framework for managing a resource-constrained,
potentially inter-dependent collection of concurrent tasks.

There are three basic elements in taskpool: Tasks, Handles that refer to
registered tasks, a Pool to holds the tasks, and a Manager to execute the
tasks.

Tasks form a partially ordered set, which ordered establishes serial
execution, and lack of ordering allows for concurrent execution.

Results of a task are reported through its Handle, which may be waited on,
polled, or canceled.  Exceptions during task execution are reported back to
the caller in the same way as the Async type.  In fact, a Handle simply
contains a TMVar that references an Async value.

The basic operations of taskpool are:

  createPool  :: Int -> IO Pool
  suspendPool :: Pool -> IO ()
  resumePool  :: Pool -> IO ()
  cancelAll   :: Pool -> IO ()

  submitTask          :: Pool -> Task -> IO Handle
  submitDependentTask :: Pool -> Handle -> Task -> IO Handle
  cancelTask          :: Handle -> IO ()
  waitOnTask          :: Handle -> IO a
  waitOnTaskEither    :: Exception e => Handle -> IO (Either e a)
  pollTask            :: Handle -> IO (Maybe a)
  pollTaskEither      :: Exception e => Handle -> IO (Maybe (Either e a))
