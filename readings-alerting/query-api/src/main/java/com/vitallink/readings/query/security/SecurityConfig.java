package com.vitallink.readings.query.security;

import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;

import jakarta.servlet.http.HttpServletResponse;

/**
 * Bearer tokens from the dev issuer: HS256 with a shared secret, issuer and audience checked, plus the VitalLink
 * claims (vl_actor, numeric coach subject). Production swaps the decoder for the OIDC issuer's JWKS; nothing else
 * changes. Every /v1 and /internal endpoint needs a valid token; scopes and panels are checked in the controllers
 * (PanelAccessGuard) so refusals can be audited with the patient they concern.
 */
@Configuration
public class SecurityConfig {

    static final int MIN_SECRET_BYTES = 32;

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health/**", "/actuator/health").permitAll()
                        .requestMatchers("/error").permitAll()
                        .anyRequest().authenticated())
                .oauth2ResourceServer(o -> o
                        .jwt(jwt -> { })
                        .authenticationEntryPoint((request, response, e) ->
                                problem(response, HttpStatus.UNAUTHORIZED, "UNAUTHENTICATED",
                                        "A valid bearer token is required"))
                        .accessDeniedHandler((request, response, e) ->
                                problem(response, HttpStatus.FORBIDDEN, "ACCESS_DENIED", "Access denied")));
        return http.build();
    }

    @Bean
    JwtDecoder jwtDecoder(@Value("${vitallink.security.jwt.secret}") String secret,
                          @Value("${vitallink.security.jwt.issuer}") String issuer,
                          @Value("${vitallink.security.jwt.audience}") String audience) {
        byte[] key = secret.getBytes(StandardCharsets.UTF_8);
        if (key.length < MIN_SECRET_BYTES) {
            throw new IllegalStateException("vitallink.security.jwt.secret (VITALLINK_DEV_JWT_SECRET) must be at least "
                    + MIN_SECRET_BYTES + " bytes");
        }
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withSecretKey(new SecretKeySpec(key, "HmacSHA256"))
                .macAlgorithm(MacAlgorithm.HS256)
                .build();
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefaultWithIssuer(issuer), audience(audience), SecurityConfig::vitallinkClaims));
        return decoder;
    }

    private static OAuth2TokenValidator<Jwt> audience(String audience) {
        return jwt -> {
            List<String> aud = jwt.getAudience();
            return aud != null && aud.contains(audience) ? OAuth2TokenValidatorResult.success()
                    : invalid("Token audience does not include " + audience);
        };
    }

    private static OAuth2TokenValidatorResult vitallinkClaims(Jwt jwt) {
        Caller.Kind kind;
        try {
            kind = Caller.Kind.valueOf(String.valueOf(jwt.getClaimAsString(Caller.ACTOR_CLAIM)));
        } catch (IllegalArgumentException e) {
            return invalid("Unknown " + Caller.ACTOR_CLAIM);
        }
        String sub = jwt.getSubject();
        if (sub == null || sub.isBlank()) {
            return invalid("Missing sub");
        }
        if (kind == Caller.Kind.COACH && !sub.matches("[1-9][0-9]{0,17}")) {
            return invalid("Coach sub must be the numeric coach id");
        }
        return OAuth2TokenValidatorResult.success();
    }

    private static OAuth2TokenValidatorResult invalid(String description) {
        return OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token", description, null));
    }

    private static void problem(HttpServletResponse response, HttpStatus status, String code, String detail)
            throws java.io.IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        if (status == HttpStatus.UNAUTHORIZED) {
            response.setHeader("WWW-Authenticate", "Bearer");
        }
        response.getWriter().write("""
                {"type":"/problems/%s","title":"%s","status":%d,"detail":"%s","code":"%s"}"""
                .formatted(code.toLowerCase().replace('_', '-'), status.getReasonPhrase(), status.value(), detail,
                        code));
    }
}
