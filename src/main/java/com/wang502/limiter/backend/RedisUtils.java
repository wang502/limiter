package com.wang502.limiter.backend;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Created by Xiaohui on 4/14/17.
 */
public class RedisUtils {

    public static <Resp> Resp ExecuteWithCallback(JedisPool jedisPool, RedisCallback<Jedis, Resp> cb){
        Jedis conn = null;
        boolean gotJedisConnException = false;

        try{
            conn = jedisPool.getResource();
            return cb.apply(conn);
        } catch (JedisConnectionException e) {
            gotJedisConnException = true;
            throw(e);
        } finally {

        }
    }
}
