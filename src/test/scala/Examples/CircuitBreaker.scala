import RedisCircuitBreaker.RedisCircuitBreakerOpen
import _root_.Util.ProtobufCodecSupplier
import nl.vroste.rezilience.*
import zio.*
import zio.redis.embedded.EmbeddedRedis
import zio.redis.{Redis, RedisExecutor}

object CircuitBreaker extends ZIOAppDefault {

  // We use Throwable as error type in this example
  private def callExternalSystem(someInput: String): ZIO[Any, Throwable, Boolean] = for {
    rand    <- ZIO.random
    success <- rand.nextBoolean
    output  <- {
      if (success) ZIO.succeed(success)
      else ZIO.fail(new RuntimeException(s"External system failed $someInput"))
    }
  } yield output

  private val circuitBreakerLayer: ZLayer[Scope & Redis, Throwable, RedisCircuitBreaker[Any]] = ZLayer {
    for {
      redis          <- ZIO.service[Redis]
      circuitBreaker <- RedisCircuitBreaker.make(
        "my-circuit-breaker",
        redis,
        trippingStrategy = TrippingStrategy.failureCount(maxFailures = 3),
        resetPolicy = Retry.Schedules.exponentialBackoff(min = 1.second, max = 1.minute),
      )
    } yield circuitBreaker
  }

  override def run: ZIO[Environment & ZIOAppArgs & Scope, Any, Any] = {
    ZIO.serviceWithZIO[RedisCircuitBreaker[Throwable]] { cb =>
      cb(callExternalSystem("some input"))
        .flatMap(r => ZIO.logInfo(s"External system returned $r"))
        .catchSome {
          case RedisCircuitBreakerOpen             =>
            ZIO.logInfo("Circuit breaker blocked the call to our external system")
          case RedisCircuitBreaker.WrappedError(e) =>
            ZIO.logInfo(s"External system threw an exception: $e")
        }
    }
  }.delay(100.milliseconds)
    .repeatN(20)
    .provideSome[Scope](
      ProtobufCodecSupplier.layer,
      EmbeddedRedis.layer,
      RedisExecutor.layer,
      Redis.layer,
      circuitBreakerLayer,
    )

}
