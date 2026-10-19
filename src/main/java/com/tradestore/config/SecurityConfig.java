package com.tradestore.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * REST endpoints are an OAuth2 resource server: every request carries a JWT bearer token, and the token's scopes
 * decide what the caller may do. {@code POST /api/v1/trades} needs {@code trades:write}; read endpoints need
 * {@code trades:read}. Actuator health and info are public so that load balancers can probe them.
 * <p>
 * The API is stateless and uses no cookies, so sessions and CSRF protection are disabled.
 */
@Configuration
public class SecurityConfig {

    static final String WRITE_SCOPE = "SCOPE_trades:write";
    static final String READ_SCOPE = "SCOPE_trades:read";

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .authorizeHttpRequests(requests -> requests
                        .requestMatchers("/actuator/health/**", "/actuator/info").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/v1/trades").hasAuthority(WRITE_SCOPE)
                        .requestMatchers(HttpMethod.GET, "/api/v1/**").hasAuthority(READ_SCOPE)
                        .anyRequest().authenticated())
                .oauth2ResourceServer(resourceServer -> resourceServer.jwt(Customizer.withDefaults()))
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .csrf(AbstractHttpConfigurer::disable)
                .build();
    }
}
