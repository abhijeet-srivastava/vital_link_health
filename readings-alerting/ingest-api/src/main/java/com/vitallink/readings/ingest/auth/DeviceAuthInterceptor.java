package com.vitallink.readings.ingest.auth;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import com.vitallink.readings.ports.DeviceAuthenticator;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/** Authenticates before the body is read, so a bad key is 401 whatever the body (not 413 or 422). */
@Component
public class DeviceAuthInterceptor implements HandlerInterceptor {

    public static final String HEADER = "X-Device-Key";
    public static final String DEVICE_ATTRIBUTE = "com.vitallink.readings.ingest.device";

    private final DeviceAuthenticator authenticator;

    public DeviceAuthInterceptor(DeviceAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        request.setAttribute(DEVICE_ATTRIBUTE, authenticator.authenticate(request.getHeader(HEADER)));
        return true;
    }
}
