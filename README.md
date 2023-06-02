# zio-redis-rezilience
Proof of Concept ZIO Redis version of zio Rezilience (double secret pre-alpha :P) use at your own risk.

* ZIO Rezilience
 * https://github.com/svroonland/rezilience
* ZIO Redis
 * https://github.com/zio/zio-redis

## Current Components
* Redis Circuit Breaker
* Redis Rate Limiter (sleeping/delay)
  * Broken out into two components.
    * RedisRateLimiter that functions similar to ZIO Rezilience RateLimiter but it uses the RedisTokenBucket to do the actual limit decisions. 
    * RedisTokenBucket that isolates the allow function into ZIO success/failure channels that could be used to build different limiting methods such as effect failing, dropping and not just streaming with delay.


## Notes
ZIO Redis doesn't support transactions or pipelining
