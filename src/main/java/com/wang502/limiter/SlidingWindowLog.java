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
public class SlidingWindowLog implements LimiterService {

    private LimiterRedisBackend redisBackend;
    private Configuration config;
    private String name;

    public SlidingWindowLog(LimiterRedisBackend redisBackend, Configuration limiterConfig){
        this.redisBackend = redisBackend;
        this.config = limiterConfig;
        this.name = "SlidingWindowLog";
    }

    public boolean processRequest(String userKey, double timestamp) {
        // time span in Unix of the limiter, 1 min, 1 hour etc
        double timespan = this.config.getDouble("TIME_SPAN");
        System.out.println(timespan);
        // limit of visits with a time span
        double limit = config.getDouble("LIMIT");

        String member = String.valueOf(timestamp);
        Long addResp = this.redisBackend.addSortedSet(userKey, timestamp, member);
        Long removeResp = this.redisBackend.removeSortedSetByScore(userKey, 0, timestamp - timespan);
        System.out.println(removeResp);
        Long size = this.redisBackend.sortedSetSize(userKey);

        if (size <= limit) {
            return true;
        }

        return false;
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
        File propertiesFile2 = new File("/Users/Xiaohui/Desktop/contribution/limiter/src/main/config/limiter.properties");
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
        LimiterService service = new SlidingWindowLog(redisBackend, limiterConfig);

        System.out.println(System.currentTimeMillis() / 1000L);

        boolean ok1 = service.processRequest("0415", System.currentTimeMillis() / 1000L);
        TimeUnit.SECONDS.sleep(1);
        boolean ok2 = service.processRequest("0415", System.currentTimeMillis() / 1000L);
        TimeUnit.SECONDS.sleep(1);
        boolean ok3 = service.processRequest("0415", System.currentTimeMillis() / 1000L);
        TimeUnit.SECONDS.sleep(1);
        boolean ok4 = service.processRequest("0415", System.currentTimeMillis() / 1000L);
        TimeUnit.SECONDS.sleep(1);
        boolean ok5 = service.processRequest("0415", System.currentTimeMillis() / 1000L);
        TimeUnit.SECONDS.sleep(1);
        boolean ok6 = service.processRequest("0415", System.currentTimeMillis() / 1000L);

        System.out.println(ok1 + " " + ok2 + " " + ok3 + " " + ok4 + " " + ok5 + " " + ok6);
    }
}
