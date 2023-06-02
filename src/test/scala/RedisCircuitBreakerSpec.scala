import RedisCircuitBreaker.{RedisCircuitBreakerOpen, State, WrappedError}
import Util.ProtobufCodecSupplier
import zio.*
import zio.redis.embedded.EmbeddedRedis
import zio.redis.{Redis, RedisExecutor}
import zio.test.*
import zio.test.Assertion.*

object CircuitBreakerSpec extends ZIOSpecDefault {
  sealed trait Error
  case object MyCallError     extends Error
  case object MyNotFatalError extends Error

  val testKey = "test-circuit-breaker"

  val isFailure: PartialFunction[Error, Boolean] = {
    case MyNotFatalError => false
    case _: Error        => true
  }

  // TODO add generator based checks with different nr of parallel calls to check
  // for all kinds of race conditions
  override def spec = suite("RedisCircuitBreaker")(
    test("lets successful calls through") {
      for {
        redis <- ZIO.service[Redis]
        cb    <- RedisCircuitBreaker.withMaxFailures(testKey, redis, 10, Schedule.exponential(1.second))
        _     <- cb(ZIO.unit).repeat(Schedule.recurs(20))
      } yield assertCompletes
    },
    test("fails fast after max nr failures calls") {

      for {
        redis  <- ZIO.service[Redis]
        cb     <- RedisCircuitBreaker.withMaxFailures(testKey, redis, 100, Schedule.exponential(1.second))
        _      <-
          ZIO.foreachParDiscard(1 to 105)(_ => cb(ZIO.fail(MyCallError)).either.tapErrorCause(c => ZIO.debug(c)))
        result <- cb(ZIO.fail(MyCallError)).either
      } yield assert(result)(isLeft(equalTo(RedisCircuitBreaker.RedisCircuitBreakerOpen)))
    } @@ TestAspect.diagnose(20.seconds),
    test("ignore failures that should not be considered a failure") {
      for {
        redis  <- ZIO.service[Redis]
        cb     <- RedisCircuitBreaker.withMaxFailures(testKey, redis, 3, Schedule.exponential(1.second), isFailure)
        _      <- cb(ZIO.fail(MyNotFatalError)).either.repeatN(3)
        result <- cb(ZIO.fail(MyCallError)).either
      } yield assertTrue(
        result.left.toOption.get != RedisCircuitBreaker.RedisCircuitBreakerOpen,
      )
    },
    test("reset to closed state after reset timeout") {
      for {
        redis        <- ZIO.service[Redis]
        stateChanges <- Queue.unbounded[State]
        cb           <- RedisCircuitBreaker.withMaxFailures(
          testKey,
          redis,
          10,
          Schedule.exponential(1.second),
          onStateChange = stateChanges.offer(_).ignore,
        )
        _            <- ZIO.foreachDiscard(1 to 10)(_ => cb(ZIO.fail(MyCallError)).either)
        _            <- stateChanges.take
        _            <- TestClock.adjust(3.second)
        _            <- stateChanges.take
        _            <- cb(ZIO.unit)
      } yield assertCompletes
    },
    test("retry exponentially") {
      (for {
        redis        <- ZIO.service[Redis]
        stateChanges <- Queue.unbounded[State]
        cb           <- RedisCircuitBreaker.withMaxFailures(
          testKey,
          redis,
          3,
          Schedule.exponential(base = 1.second, factor = 2.0),
          onStateChange = stateChanges.offer(_).ignore,
        )
        _            <- ZIO.foreachDiscard(1 to 3)(_ => cb(ZIO.fail(MyCallError)).either)
        s1           <- stateChanges.take // Open
        _            <- TestClock.adjust(1.second)
        s2           <- stateChanges.take // HalfOpen
        _            <- cb(ZIO.fail(MyCallError)).either
        s3           <- stateChanges.take // Open again
        s4           <- stateChanges.take.timeout(1.second) <& TestClock.adjust(1.second)
        _            <- TestClock.adjust(1.second)
        s5           <- stateChanges.take
        _            <- cb(ZIO.unit)
        s6           <- stateChanges.take
      } yield assertTrue(s1 == State.Open, s2 == State.HalfOpen, s3 == State.Open) &&
        assert(s4)(isNone) &&
        assertTrue(s5 == State.HalfOpen) &&
        assert(s6)(equalTo(State.Closed))).tapErrorCause(result => ZIO.debug(result))
    },
    test("reset the exponential timeout after a Closed-Open-HalfOpen-Closed") {
      for {
        redis        <- ZIO.service[Redis]
        stateChanges <- Queue.unbounded[State]
        cb           <- RedisCircuitBreaker.withMaxFailures(
          testKey,
          redis,
          3,
          Schedule.exponential(base = 1.second, factor = 2.0),
          onStateChange = stateChanges.offer(_).ignore,
        )

        _ <- ZIO.foreachDiscard(1 to 3)(_ => cb(ZIO.fail(MyCallError)).either)
        _ <- stateChanges.take // Open
        _ <- TestClock.adjust(1.second)
        _ <- stateChanges.take // HalfOpen

        _ <- cb(ZIO.fail(MyCallError)).either
        _ <- stateChanges.take // Open again, this time with double reset timeout

        _ <- TestClock.adjust(2.second)
        _ <- stateChanges.take // HalfOpen

        _ <- cb(ZIO.unit)
        _ <- stateChanges.take // Closed again

        _ <- ZIO.foreachDiscard(1 to 3)(_ => cb(ZIO.fail(MyCallError)).either)
        _ <- stateChanges.take // Open

        // Reset time should have re-initialized again
        _  <- TestClock.adjust(1.second)
        s1 <- stateChanges.take // HalfOpen
      } yield assertTrue(s1 == State.HalfOpen)
    },
    test("reset to Closed after Half-Open on success")(
      for {
        redis   <- ZIO.service[Redis]
        cb      <- RedisCircuitBreaker.withMaxFailures(testKey, redis, 5, Schedule.exponential(2.second), isFailure)
        intRef  <- Ref.make(0)
        error1  <- cb(ZIO.fail(MyNotFatalError)).flip
        errors  <- ZIO.replicateZIO(5)(cb(ZIO.fail(MyCallError)).flip)
        _       <- TestClock.adjust(1.second)
        error3  <- cb(intRef.update(_ + 1)).flip // no backend calls here
        _       <- TestClock.adjust(1.second)
        _       <- cb(intRef.update(_ + 1))
        _       <- cb(intRef.update(_ + 1))
        nrCalls <- intRef.get
      } yield assertTrue(
        error1.asInstanceOf[WrappedError[Error]].error == MyNotFatalError,
        errors.forall(_.asInstanceOf[WrappedError[Error]].error == MyCallError),
        error3 == RedisCircuitBreakerOpen,
        nrCalls == 2,
      ),
    ),
    test("reset to Closed after Half-Open on error if isFailure=false") {
      for {
        redis   <- ZIO.service[Redis]
        cb      <- RedisCircuitBreaker.withMaxFailures(testKey, redis, 5, Schedule.exponential(2.second), isFailure)
        intRef  <- Ref.make(0)
        errors  <- ZIO.replicateZIO(5)(cb(ZIO.fail(MyCallError)).flip)
        _       <- TestClock.adjust(1.second)
        error1  <- cb(intRef.update(_ + 1)).flip // no backend calls here
        _       <- TestClock.adjust(1.second)
        error2  <- cb(ZIO.fail(MyNotFatalError)).flip
        _       <- cb(intRef.update(_ + 1))
        nrCalls <- intRef.get
      } yield assertTrue(
        errors.forall(_.asInstanceOf[WrappedError[Error]].error == MyCallError),
        error1 == RedisCircuitBreakerOpen,
        error2.asInstanceOf[WrappedError[Error]].error == MyNotFatalError,
        nrCalls == 1,
      )
    },
  ).provideSome[Scope](
    ProtobufCodecSupplier.layer,
    EmbeddedRedis.layer,
    RedisExecutor.layer,
    Redis.layer,
  )

}
