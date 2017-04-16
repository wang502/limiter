package com.wang502.limiter.backend;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
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

    //
    // Sorted set methods
    //
    public Long addSortedSet(final String key, final double score, final String member) {
        try {
            Long resp = RedisUtils.ExecuteWithCallback(this.redisPool.getGeneralRedisPool(), new RedisCallback<Jedis, Long>() {
                public Long apply(Jedis conn) {
                    return conn.zadd(key, score, member);
                }
            });
            return resp;

        } catch (JedisConnectionException e) {
            // retry
            return addSortedSet(key, score, member);
        }
    }

    public Long removeSortedSetByScore(final String key, final double start, final double end) {
        try {
            Long resp = RedisUtils.ExecuteWithCallback(this.redisPool.getGeneralRedisPool(), new RedisCallback<Jedis, Long>() {
                public Long apply(Jedis conn) {
                    return conn.zremrangeByScore(key, start, end);
                }
            });
            return resp;
        } catch (JedisConnectionException e) {
            // retry
            return removeSortedSetByScore(key, start, end);
        }
    }

    public Long sortedSetSize(final String key) {
        try {
            Long resp = RedisUtils.ExecuteWithCallback(this.redisPool.getGeneralRedisPool(), new RedisCallback<Jedis, Long>() {
                public Long apply(Jedis conn) {
                    return conn.zcard(key);
                }
            });
            return resp;
        } catch (JedisConnectionException e) {
            // retry
            return sortedSetSize(key);
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
