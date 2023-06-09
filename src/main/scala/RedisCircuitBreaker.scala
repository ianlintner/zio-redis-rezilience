import RedisCircuitBreaker.RedisCircuitBreakerCallError
import Util.RedisRef
import nl.vroste.rezilience.Policy.PolicyError
import nl.vroste.rezilience.{Policy, Retry, TrippingStrategy}
import zio.*
import zio.redis.Redis
import zio.schema.{DeriveSchema, Schema}
import zio.stream.ZStream

/** RedisCircuitBreaker protects external resources against overload under failure
  *
  * Operates in three states:
  *
  *   - Closed (initial state / normal operation): calls are let through normally. Call failures and successes update the call statistics, eg failure count.
  *     When the statistics satisfy some criteria, the circuit breaker is 'tripped' and set to the Open state. Note that after this switch, in-flight calls are
  *     not canceled. Their success or failure does not affect the circuit breaker anymore though.
  *
  *   - Open: all calls fail fast with a `RedisCircuitBreakerOpen` error. After the reset timeout, the states changes to HalfOpen
  *
  *   - HalfOpen: the first call is let through. Meanwhile all other calls fail with a `RedisCircuitBreakerOpen` error. If the first call succeeds, the state
  *     changes to Closed again (normal operation). If it fails, the state changes back to Open. The reset timeout is governed by a reset policy, which is
  *     typically an exponential backoff.
  *
  * Two tripping strategies are implemented: 1) Failure counting. When the number of successive failures exceeds a threshold, the circuit breaker is tripped.
  *
  * Note that the maximum number of failures before tripping the circuit breaker is not absolute under concurrent execution. I.e. if you make 20 calls to a
  * failing system in parallel via a circuit breaker with max 10 failures, the calls will be running concurrently. The circuit breaker will trip after 10 calls,
  * but the remaining 10 that are in-flight will continue to run and fail as well.
  *
  * TODO what to do if you want this kind of behavior, or should we make it an option?
  *
  * 2) Failure rate. When the fraction of failed calls in some sample period exceeds a threshold (between 0 and 1), the circuit breaker is tripped. The decision
  * to trip the Circuit Breaker is made after every call (including successful ones!)
  */
trait RedisCircuitBreaker[-E] {
  self =>

  /** Execute a given effect with the circuit breaker
    *
    * @param f
    *   Effect to execute
    * @return
    *   A ZIO that either succeeds with the success of the given f or fails with either a `RedisCircuitBreakerOpen` or a `WrappedError` of the error of the
    *   given f
    */
  def apply[R, E1 <: E, A](f: ZIO[R, E1, A]): ZIO[R, RedisCircuitBreakerCallError[E1], A]

  def toPolicy: Policy[E] = new Policy[E] {

    override def apply[R, E1 <: E, A](f: ZIO[R, E1, A]): ZIO[R, PolicyError[E1], A] =
      self(f).mapError(_.fold(Policy.CircuitBreakerOpen, Policy.WrappedError(_)))

  }

  /** Transform this policy to apply to larger class of errors
    *
    * Only for errors where the partial function is defined will errors be considered as failures, otherwise the error is passed through to the caller
    *
    * @param pf
    *   Map an error of type E2 to an error of type E
    * @tparam E2
    * @return
    *   A new RedisCircuitBreaker defined for failures of type E2
    */
  def widen[E2](pf: PartialFunction[E2, E]): RedisCircuitBreaker[E2]
}

object RedisCircuitBreaker {

  import State.*

  implicit val stateSchema: Schema[State] = DeriveSchema.gen[State]

  sealed trait RedisCircuitBreakerCallError[+E] { self =>
    def toException: Exception = RedisCircuitBreakerException(self)

    def fold[O](circuitBreakerOpen: O, unwrap: E => O): O = self match {
      case RedisCircuitBreakerOpen => circuitBreakerOpen
      case WrappedError(error)     => unwrap(error)
    }

  }

  case object RedisCircuitBreakerOpen  extends RedisCircuitBreakerCallError[Nothing]
  case class WrappedError[E](error: E) extends RedisCircuitBreakerCallError[E]

  case class RedisCircuitBreakerException[E](error: RedisCircuitBreakerCallError[E]) extends Exception("Circuit breaker error")

  sealed trait State

  object State {
    case object Closed   extends State
    case object HalfOpen extends State
    case object Open     extends State
  }

  /** Create a RedisCircuitBreaker that fails when a number of successive failures (no pun intended) has been counted
    *
    * @param maxFailures
    *   Maximum number of failures before tripping the circuit breaker
    * @param resetPolicy
    *   Reset schedule after too many failures. Typically an exponential backoff strategy is used.
    * @param isFailure
    *   Only failures that match according to `isFailure` are treated as failures by the circuit breaker. Other failures are passed on, circumventing the
    *   circuit breaker's failure counter.
    * @param onStateChange
    *   Observer for circuit breaker state changes
    * @return
    *   The RedisCircuitBreaker as a managed resource
    */
  def withMaxFailures[E](
    key: String,
    redis: Redis,
    maxFailures: Int,
    resetPolicy: Schedule[Any, Any, Any] = Retry.Schedules.exponentialBackoff(1.second, 1.minute),
    isFailure: PartialFunction[E, Boolean] = isFailureAny[E],
    onStateChange: State => UIO[Unit] = _ => ZIO.unit,
  ): ZIO[Scope, Throwable, RedisCircuitBreaker[E]] =
    make(key, redis, TrippingStrategy.failureCount(maxFailures), resetPolicy, isFailure, onStateChange)

  /** Create a RedisCircuitBreaker with the given tripping strategy
    * @param keyPrefix
    *   to have shared keys for servers and avoid collisions
    * @param trippingStrategy
    *   Determines under which conditions the CircuitBraker trips
    * @param resetPolicy
    *   Reset schedule after too many failures. Typically an exponential backoff strategy is used.
    * @param isFailure
    *   Only failures that match according to `isFailure` are treated as failures by the circuit breaker. Other failures are passed on, circumventing the
    *   circuit breaker's failure counter.
    * @param onStateChange
    *   Observer for circuit breaker state changes
    * @return
    */
  def make[E](
    key: String,
    redis: Redis,
    trippingStrategy: ZIO[Scope, Nothing, TrippingStrategy],
    resetPolicy: Schedule[Any, Any, Any] = Retry.Schedules.exponentialBackoff(1.second, 2.minute),
    isFailure: PartialFunction[E, Boolean] = isFailureAny[E],
    onStateChange: State => UIO[Unit] = _ => ZIO.unit,
  ): ZIO[Scope, Throwable, RedisCircuitBreaker[E]] =
    for {
      strategy       <- trippingStrategy
      state          <- RedisRef.make[State](s"$key:state", Closed, redis)
      halfOpenSwitch <- RedisRef.make[Boolean](s"$key:halfOpenSwitch", true, redis)
      schedule       <- resetPolicy.driver
      resetRequests  <- Queue.bounded[Unit](1)
      _              <- ZStream
        .fromQueue(resetRequests)
        .mapZIO { _ =>
          for {
            _ <- schedule.next(())            // TODO handle schedule completion?
            _ <- halfOpenSwitch.set(true)
            _ <- state.set(HalfOpen)
            _ <- onStateChange(HalfOpen).fork // Do not wait for user code
          } yield ()
        }
        .runDrain
        .forkScoped
    } yield new RedisCircuitBreakerImpl[resetPolicy.State, E](
      state,
      resetRequests,
      strategy,
      onStateChange,
      schedule,
      isFailure,
      halfOpenSwitch,
    )

  private case class RedisCircuitBreakerImpl[ScheduleState, -E](
    state: RedisRef[State],
    resetRequests: Queue[Unit],
    strategy: TrippingStrategy,
    onStateChange: State => UIO[Unit],
    schedule: Schedule.Driver[ScheduleState, Any, Any, Any],
    isFailure: PartialFunction[E, Boolean],
    halfOpenSwitch: RedisRef[Boolean],
  ) extends RedisCircuitBreaker[E] {

    val changeToOpen = ZIO.logDebug("Change to open") *>
      state.set(Open) *>
      resetRequests.offer(()) <*
      onStateChange(Open).fork // Do not wait for user code

    val changeToClosed = ZIO.logDebug("Change to open") *>
      strategy.onReset *>
      schedule.reset *>
      state.set(Closed) <*
      onStateChange(Closed).fork // Do not wait for user code

    override def apply[R, E1 <: E, A](f: ZIO[R, E1, A]): ZIO[R, RedisCircuitBreakerCallError[E1], A] =
      for {
        currentState <- state.get
        result       <- currentState match {
          case Closed =>
            // The state may have already changed to Open or even HalfOpen.
            // This can happen if we fire X calls in parallel where X >= 2 * maxFailures
            def onComplete(callSuccessful: Boolean) =
              (for {
                shouldTrip   <- strategy.shouldTrip(callSuccessful)
                currentState <- state.get
                _            <- ZIO.logDebug(s"shouldTrip: $shouldTrip, currentState: $currentState, callSuccessful: $callSuccessful")
                _            <- changeToOpen.when(currentState == Closed && shouldTrip)
              } yield ()).uninterruptible

            tapZIOOnUserDefinedFailure(f)(
              onFailure = onComplete(callSuccessful = false),
              onSuccess = onComplete(callSuccessful = true),
            ).mapError(WrappedError(_))

          case Open     =>
            ZIO.fail(RedisCircuitBreakerOpen)
          case HalfOpen =>
            for {
              isFirstCall <- halfOpenSwitch.getSet(false)
              _           <- ZIO.logDebug(s"HalfOpen: isFirstCall: $isFirstCall")
              result      <-
                if (isFirstCall) {
                  tapZIOOnUserDefinedFailure(f)(
                    onFailure = (strategy.shouldTrip(false) *> changeToOpen).uninterruptible,
                    onSuccess = (changeToClosed *> strategy.onReset).uninterruptible,
                  ).mapError(WrappedError(_))
                } else {
                  ZIO.fail(RedisCircuitBreakerOpen)
                }
            } yield result
        }
      } yield result

    private def tapZIOOnUserDefinedFailure[R, E1 <: E, A](
      f: ZIO[R, E1, A],
    )(onFailure: ZIO[R, E1, Any], onSuccess: ZIO[R, E1, Any]): ZIO[R, E1, A] =
      f.tapBoth(
        {
          case e if isFailure.applyOrElse[E1, Boolean](e, _ => false) =>
            onFailure
          case _                                                      =>
            onSuccess
        },
        _ => onSuccess,
      )

    def widen[E2](pf: PartialFunction[E2, E]): RedisCircuitBreaker[E2] = RedisCircuitBreakerImpl[ScheduleState, E2](
      state,
      resetRequests,
      strategy,
      onStateChange,
      schedule,
      pf andThen isFailure,
      halfOpenSwitch,
    )

  }

  def isFailureAny[E]: PartialFunction[E, Boolean] = { case _ => true }

}
