import Exceptions.RateLimitExceededException
import zio.redis.Redis
import zio.{durationInt, Clock, Duration, Task, ZIO}

case class RedisTokenBucketRateLimiter(redis: Redis, key: String, maxRequests: Long, duration: Duration, expiration: Option[Duration] = Option(1.day)) {
  private val bucketKey: String     = s"bucket:$key"
  private val requestKey: String    = s"request:$key"
  private val intervalSeconds: Long = duration.toSeconds

  def allow: Task[Long] = {
    for {
      now        <- Clock.instant
      bucketSize <- redis.get(bucketKey).returning[Long].someOrElse(maxRequests)
      lastRefill <- redis.get(requestKey).returning[Long].someOrElseZIO(redis.set(requestKey, now.getEpochSecond).as(now.getEpochSecond))
      _          <- ZIO.logDebug(s"bucketSize: $bucketSize, lastRefill: $lastRefill")
      result     <- (bucketSize, now.getEpochSecond - lastRefill >= intervalSeconds) match {
        case (0, false) => ZIO.fail(new RateLimitExceededException(s"Rate limit exceeded $bucketSize of $maxRequests requests in $intervalSeconds seconds"))
        case (_, true)  => redis.set(requestKey, now.getEpochSecond) &> redis.set(bucketKey, maxRequests - 1) *> ZIO.succeed(bucketSize)
        case (_, false) => redis.set(bucketKey, bucketSize - 1) *> ZIO.succeed(bucketSize)
      }
    } yield (result)
  }

}
