package io.zeroshift.platform.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Gives every request an id: the caller's {@code X-Request-Id} when it is a sane token (so one id
 * follows a request across services), otherwise a new one. It is echoed in the response, put in the
 * logging MDC, and included in every error body.
 */
public class RequestIdFilter extends OncePerRequestFilter {
  private static final Pattern ACCEPTED = Pattern.compile("[A-Za-z0-9._:-]{1,100}");

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    var given = request.getHeader(ApiHeaders.REQUEST_ID);
    var id =
        given != null && ACCEPTED.matcher(given).matches() ? given : UUID.randomUUID().toString();
    MDC.put(RequestContext.REQUEST_ID, id);
    response.setHeader(ApiHeaders.REQUEST_ID, id);
    try {
      chain.doFilter(request, response);
    } finally {
      MDC.remove(RequestContext.REQUEST_ID);
    }
  }
}
