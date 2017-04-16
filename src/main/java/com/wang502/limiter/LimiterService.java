package com.wang502.limiter;

/**
 * Created by Xiaohui on 4/15/17.
 */
public interface LimiterService {
    boolean processRequest(String userKey, double timestamp);
    String getServiceName();
}
