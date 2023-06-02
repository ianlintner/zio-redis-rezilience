package Exceptions

case class RateLimitExceededException(message: String) extends RuntimeException(message)
