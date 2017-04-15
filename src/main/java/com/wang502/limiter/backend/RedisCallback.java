package com.wang502.limiter.backend;

/**
 * Created by Xiaohui on 4/14/17.
 */
public interface RedisCallback<Jedis, Resp> {
    Resp apply(Jedis conn);
}
