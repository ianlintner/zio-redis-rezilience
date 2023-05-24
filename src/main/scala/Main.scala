import nl.vroste.rezilience.*
import nl.vroste.rezilience.RedisCircuitBreaker.*
import zio.*
import zio.redis.embedded.EmbeddedRedis
import zio.redis.{CodecSupplier, Redis, RedisExecutor}
import zio.schema.Schema
import zio.schema.codec.{BinaryCodec, ProtobufCodec}

object Main extends ZIOAppDefault {

  object ProtobufCodecSupplier extends CodecSupplier {
    def get[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec
  }

  // We use Throwable as error type in this example
  private def callExternalSystem(someInput: String): ZIO[Any, Throwable, Boolean] = for {
    rand    <- ZIO.random
    success <- rand.nextBoolean
    output  <- {
      if (success) ZIO.succeed(success)
      else ZIO.fail(new RuntimeException(s"External system failed $someInput"))
    }
  } yield output

  private val circuitBreakerLayer: ZLayer[Scope with Redis, Throwable, RedisCircuitBreaker[Throwable]] = ZLayer {
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

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] = {
    ZIO.serviceWithZIO[RedisCircuitBreaker[Throwable]] { cb =>
      cb(callExternalSystem("some input"))
        .flatMap(r => ZIO.logInfo(s"External system returned $r"))
        .catchSome {
          case RedisCircuitBreakerOpen =>
            ZIO.logInfo("Circuit breaker blocked the call to our external system")
          case WrappedError(e)         =>
            ZIO.logInfo(s"External system threw an exception: $e")
        }
    }
  }.delay(100.milliseconds)
    .repeatN(20)
    .provideSome[Scope](
      ZLayer.succeed[CodecSupplier](ProtobufCodecSupplier),
      EmbeddedRedis.layer,
      RedisExecutor.layer,
      Redis.layer,
      circuitBreakerLayer,
    )

}
