package com.wang502.limiter;

import com.wang502.limiter.backend.LimiterRedisBackend;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Created by Xiaohui on 4/15/17.
 */
public class SlidingWindowCounter implements LimiterService {

    private LimiterRedisBackend redisBackend;
    private Configuration config;
    private String name;

    public SlidingWindowCounter(LimiterRedisBackend redisBackend, Configuration limiterConfig){
        this.redisBackend = redisBackend;
        this.config = limiterConfig;
        this.name = "SlidingWindowCounter";
    }

    public boolean processRequest(String userKey, double timestamp) {
        String timeType = this.config.getString("TIMESTAMP_TYPE");
        if (timeType.equals("minute")){
            timestamp = (double) Math.round(timestamp/60);
        }

        return this.redisBackend.slidingWindowCounterMulti(userKey, timestamp, this.config);
    }

    public String getServiceName() {
        return this.name;
    }

    public static void main(String[] args) throws InterruptedException {
        Parameters params = new Parameters();
        File propertiesFile = new File("/Users/Xiaohui/Desktop/contribution/limiter/src/main/config/limiter.redis.properties");
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>
                        (PropertiesConfiguration.class)
                        .configure(params.properties().setFile(propertiesFile));
        Configuration redisConfig = null;
        try {
            redisConfig = builder.getConfiguration();
        }
        catch(ConfigurationException cex){
            // handle exception here
            System.out.println(cex);
        }
        String host = "127.0.0.1";
        int port = 6379;
        LimiterRedisBackend redisBackend = new LimiterRedisBackend(redisConfig, host, port);

        Parameters params2 = new Parameters();
        File propertiesFile2 = new File("/Users/Xiaohui/Desktop/contribution/limiter/src/main/config/limiter.slidingwindowcounter.properties");
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder2 =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>
                        (PropertiesConfiguration.class)
                        .configure(params2.properties().setFile(propertiesFile2));
        Configuration limiterConfig = null;
        try {
            limiterConfig = builder2.getConfiguration();
        }
        catch(ConfigurationException cex){
            // handle exception here
            System.out.println(cex);
        }
        LimiterService limiterService = new SlidingWindowCounter(redisBackend, limiterConfig);

        boolean ok1 = limiterService.processRequest("0416", System.currentTimeMillis()/1000L);
        TimeUnit.SECONDS.sleep(1);
        boolean ok2 = limiterService.processRequest("0416", System.currentTimeMillis()/1000L);
        TimeUnit.SECONDS.sleep(1);
        boolean ok3 = limiterService.processRequest("0416", System.currentTimeMillis()/1000L);
        TimeUnit.SECONDS.sleep(1);
        boolean ok4 = limiterService.processRequest("0416", System.currentTimeMillis()/1000L);
        TimeUnit.SECONDS.sleep(1);
        boolean ok5 = limiterService.processRequest("0416", System.currentTimeMillis()/1000L);

        System.out.println(ok1 + " " + ok2 + " " + ok3 + " " + ok4 + " " + ok5);


        TimeUnit.SECONDS.sleep(limiterConfig.getInt("EXPIRE"));
        boolean ok6 = limiterService.processRequest("0416", System.currentTimeMillis()/1000L);
        System.out.println(ok6);
    }
}
