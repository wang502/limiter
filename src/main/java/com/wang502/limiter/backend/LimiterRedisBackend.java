package com.wang502.limiter.backend;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.File;

/**
 * Created by Xiaohui on 4/14/17.
 */
public class LimiterRedisBackend {

    private RedisPool redisPool;

    public LimiterRedisBackend(Configuration config, String host, int port){
        this.redisPool = new RedisPool(config, host, port);
    }

    public RedisPool getRedisPool() {
        return redisPool;
    }

    public boolean slidingWindowLogMulti(final String userKey, final double timestamp, Configuration config){
        try {
            // limiter time span in seconds
            final double timespan = config.getDouble("TIME_SPAN");
            // limit of visits with a time span
            final double limit = config.getDouble("LIMIT");
            final String member = String.valueOf(timestamp);

            Long resp = RedisUtils.ExecuteWithCallback(this.redisPool.getGeneralRedisPool(), new RedisCallback<Jedis, Long>() {
                public Long apply(Jedis conn) {
                    Transaction t = conn.multi();
                    Response<Long> removeResp = t.zremrangeByScore(userKey, 0, timestamp - timespan);
                    Response<Long> addResp = t.zadd(userKey, timestamp, member);
                    Response<Long> sizeResp = t.zcard(userKey);

                    t.exec();
                    Long size = sizeResp.get();
                    return size;
                }
            });

            if (resp <= limit) {
                return true;
            }
            return false;
        } catch (JedisConnectionException e) {
            // retry
            return slidingWindowLogMulti(userKey, timestamp, config);
        }
    }

    public boolean slidingWindowCounterMulti(final String userKey, final double timestamp, Configuration config){
        try {
            // limiter time span in seconds
            final double duration = config.getDouble("EXPIRE");
            // limit of visits with a time span
            final double limit = config.getDouble("LIMIT");
            final String field = String.valueOf(timestamp);

            Long resp = RedisUtils.ExecuteWithCallback(this.redisPool.getGeneralRedisPool(), new RedisCallback<Jedis, Long>() {
                public Long apply(Jedis conn) {
                    Transaction t = conn.multi();
                    t.hincrBy(userKey, field, 1);
                    t.expire(userKey, (int) duration);
                    Response<String> valResp = t.hget(userKey, field);

                    t.exec();
                    Long count = Long.parseLong(valResp.get());
                    return count;
                }
            });

            if (resp <= limit) {
                return true;
            }
            return false;
        } catch (JedisConnectionException e){
            return slidingWindowCounterMulti(userKey, timestamp, config);
        }
    }

    public static void main(String[] args){
        Parameters params = new Parameters();
        File propertiesFile = new File("/Users/Xiaohui/Desktop/contribution/limiter/src/main/config/limiter.redis.properties");
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>
                        (PropertiesConfiguration.class)
                        .configure(params.properties().setFile(propertiesFile));
        Configuration config = null;
        try {
            config = builder.getConfiguration();
        }
        catch(ConfigurationException cex){
            // handle exception here
            System.out.println(cex);
        }

        String host = "127.0.0.1";
        int port = 6379;

        LimiterRedisBackend redisBackend = new LimiterRedisBackend(config, host, port);
        final JedisPool jedisPool = redisBackend.getRedisPool().getGeneralRedisPool();

        String resp = RedisUtils.ExecuteWithCallback(jedisPool, new RedisCallback<Jedis, String>() {
            public String apply(Jedis conn) {
                return conn.ping();
            }
        });
        System.out.println(resp);

        String setResp = RedisUtils.ExecuteWithCallback(jedisPool, new RedisCallback<Jedis, String>() {
            public String apply(Jedis conn) {
                return conn.set("foo", "barr");
            }
        });
        System.out.println(setResp);
    }
}
