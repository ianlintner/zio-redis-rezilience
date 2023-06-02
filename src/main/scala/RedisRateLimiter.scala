import Exceptions.RateLimitExceededException
import zio.*
import zio.stream.ZStream

/** Limits the number of calls to a resource to a maximum amount in some interval
  *
  * Uses a token bucket algorithm
  *
  * Note that only the moment of starting the effect is rate limited: the number of concurrent executions is not bounded. For that you may use a Bulkhead
  *
  * Calls are queued up in an unbounded queue until capacity becomes available.
  */
trait RedisRateLimiter { self =>

  /** Execute the task with RedisRateLimiter protection
    *
    * The effect returned by this method can be interrupted, which is handled as follows:
    *   - If the task is still waiting in the rate limiter queue, it will not start execution. It will also not count for the rate limiting or hold back other
    *     uninterrupted queued tasks.
    *   - If the task has already started executing, interruption will interrupt the task and will complete when the task's interruption is complete.
    *
    * @param task
    *   Task to execute. When the rate limit is exceeded, the call will be postponed.
    */
  def apply[R, E, A](task: ZIO[R, E, A]): ZIO[R, E, A]
}

object RedisRateLimiter {

  final def nextPow2(n: Int): Int = {
    val nextPow = (Math.log(n.toDouble) / Math.log(2.0)).ceil.toInt
    Math.pow(2.0, nextPow.toDouble).toInt.max(2)
  }

  /** Creates a RedisRateLimiter as Managed resource
    *
    * Note that the maximum number of calls is spread out over the interval, i.e. 10 calls per second means that 1 call can be made every 100 ms. Up to `max`
    * calls can be saved up. The maximum is immediately available when starting the RedisRateLimiter
    *
    * @param max
    *   Maximum number of calls in each interval
    * @param interval
    *   Interval duration
    * @return
    *   RedisRateLimiter
    */
  def make(rateLimiter: RedisTokenBucketRateLimiter, limiterParallelism: Int = Int.MaxValue): ZIO[Scope, Nothing, RedisRateLimiter] = {

    for {
      sem <- Semaphore.make(permits = limiterParallelism)
      q   <- Queue
        .bounded[(Ref[Boolean], UIO[Any])](
          nextPow2(rateLimiter.maxRequests.toInt),
        ) // Power of two because it is a more efficient queue implementation
      _   <- ZStream
        .fromQueue(q, maxChunkSize = 1)
        .filterZIO { case (interrupted, effect @ _) => interrupted.get.map(!_) }
        .mapZIOParUnordered(Int.MaxValue) { case (interrupted @ _, effect) =>
          effect
        }
        .runDrain
        .forkScoped
    } yield new RedisRateLimiter {
      override def apply[R, E, A](task: ZIO[R, E, A]): ZIO[R, E, A] = for {
        start                  <- Promise.make[Nothing, Unit]
        done                   <- Promise.make[Nothing, Unit]
        interruptedRef         <- Ref.make(false)
        action                  = start.succeed(()) *> done.await
        onInterruptOrCompletion = interruptedRef.set(true) *> done.succeed(())
        result                 <-
          ZIO.scoped[R] {
            ZIO.acquireReleaseInterruptible(q.offer((interruptedRef, action)).onInterrupt(onInterruptOrCompletion))(
              onInterruptOrCompletion,
            ) *> start.await *> limitDelay.orDie *> task
          }
      } yield result

      def limitDelay: ZIO[Any, Throwable, Long] = sem.withPermit(rateLimiter.allow).catchSome { case e: RateLimitExceededException =>
        ZIO.logInfo(e.message) *> limitDelay.delay(rateLimiter.duration)
      }
    }
  }

}
