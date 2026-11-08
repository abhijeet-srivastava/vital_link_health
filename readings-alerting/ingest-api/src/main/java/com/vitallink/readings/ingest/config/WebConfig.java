package com.vitallink.readings.ingest.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.vitallink.readings.ingest.auth.DeviceAuthInterceptor;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final DeviceAuthInterceptor deviceAuth;

    public WebConfig(DeviceAuthInterceptor deviceAuth) {
        this.deviceAuth = deviceAuth;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(deviceAuth).addPathPatterns("/v1/devices/**");
    }
}
