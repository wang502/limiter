package com.wang502.limiter.backend;

import org.apache.commons.configuration2.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Created by Xiaohui on 4/14/17.
 */
public class RedisPool {

    private String host;
    private int port;
    private JedisPool generalRedisPool;

    public RedisPool(Configuration config, String host, int port){
        this.host = host;
        this.port = port;
        this.generalRedisPool = createRedisPool(this.host, this.port, config.getInt("CONNECTIONS_PER_SHARD"), config.getInt("CONNECTION_MAX_WAIT_MILLIS"), config.getInt("SOCKET_TIMEOUT_MILLIS"));
    }

    public String getHost(){
        return host;
    }

    public int getPort() {
        return port;
    }

    public JedisPool getGeneralRedisPool() {
        return generalRedisPool;
    }

    private static JedisPool createRedisPool(String host, int port, int poolSize, int maxWaitMillis, int socketTimeoutMillis) {
        JedisPoolConfig config = new JedisPoolConfig();

        config.setMaxWaitMillis(maxWaitMillis);
        // config.setMaxActive(poolSize);
        config.setMaxTotal(poolSize);
        config.setMaxIdle(poolSize);

        // Deal with idle connection eviction.
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);
        config.setTestWhileIdle(true);
        config.setMinEvictableIdleTimeMillis(5 * 60 * 1000);
        config.setTimeBetweenEvictionRunsMillis(3 * 60 * 1000);
        config.setNumTestsPerEvictionRun(poolSize);

        JedisPool pool = new JedisPool(config, host, port, socketTimeoutMillis);
        // Force connection pool initialization.
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
        } catch (JedisConnectionException e) {
            System.out.println(String.format("Failed to get a redis connection when creating redis pool, "  + "host: %s, port: %d", host, port));
            throw(e);
        } finally {
            jedis.close();
        }

        return pool;
    }


}
