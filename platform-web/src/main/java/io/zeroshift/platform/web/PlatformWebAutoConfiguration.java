package io.zeroshift.platform.web;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;

/** Applied to every service that depends on platform-web: the request id and the error contract. */
@AutoConfiguration
@Import(ApiExceptionHandler.class)
public class PlatformWebAutoConfiguration {
  @Bean
  FilterRegistrationBean<RequestIdFilter> requestIdFilter() {
    var registration = new FilterRegistrationBean<>(new RequestIdFilter());
    registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
    return registration;
  }
}
