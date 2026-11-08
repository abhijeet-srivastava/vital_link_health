package com.vitallink.readings.ingest.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.vitallink.readings.ingest.auth.DeviceAuthInterceptor;
import com.vitallink.readings.ingest.backpressure.DeviceRateLimiter;
import com.vitallink.readings.ingest.backpressure.InFlightLimiter;

/**
 * Interceptors run in registration order, all before the body is read: authenticate (401), per-device rate limit
 * (429, needs the device), then load shedding (503). A rate-limited device never takes an in-flight permit.
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final DeviceAuthInterceptor deviceAuth;
    private final DeviceRateLimiter deviceRateLimiter;
    private final InFlightLimiter inFlightLimiter;

    public WebConfig(DeviceAuthInterceptor deviceAuth, DeviceRateLimiter deviceRateLimiter,
                     InFlightLimiter inFlightLimiter) {
        this.deviceAuth = deviceAuth;
        this.deviceRateLimiter = deviceRateLimiter;
        this.inFlightLimiter = inFlightLimiter;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(deviceAuth).addPathPatterns("/v1/devices/**");
        registry.addInterceptor(deviceRateLimiter).addPathPatterns("/v1/devices/**");
        registry.addInterceptor(inFlightLimiter).addPathPatterns("/v1/devices/**");
    }
}
