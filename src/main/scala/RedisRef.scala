package nl.vroste.rezilience

import zio.redis.Redis
import zio.schema.Schema
import zio.{Trace, UIO, ZIO}

object RedisRef {

  def make[A](key: String, a: => A, redis: Redis)(implicit trace: Trace, schema: Schema[A]): UIO[RedisRef[A]] = for {
    ref <- ZIO.succeed(RedisRef[A](key, a, redis))
    _   <- ref.set(a)
  } yield ref

}

case class RedisRef[A](key: String, default: A, redis: Redis) {

  def get(implicit trace: Trace, schema: Schema[A]): UIO[A] = {
    for {
      r <- redis.get(key).returning[A].tapErrorCause(ZIO.logErrorCause(_)).orDie
    } yield r.getOrElse(default)
  }

  def set(a: A)(implicit trace: Trace, schema: Schema[A]): UIO[Unit] = redis.set(key, a).tapErrorCause(ZIO.logErrorCause(_)).orDie.unit

  def getAndUpdate(f: A => A)(implicit trace: Trace, schema: Schema[A]): UIO[A] = for {
    current <- redis.get(key).returning[A].orDie.map(_.get)
    _       <- redis.set(key, f(current)).tapErrorCause(ZIO.logErrorCause(_)).orDie.unit
  } yield current

}
