package io.zeroshift.platform.web;

import java.util.Map;
import org.springframework.http.HttpStatus;

/**
 * An error raised at the HTTP boundary with its status and stable code. Application and domain code
 * throws its own exceptions; each service's advice translates them into this contract.
 */
public class ApiException extends RuntimeException {
  private final HttpStatus status;
  private final String code;
  private final Map<String, Object> context;

  public ApiException(HttpStatus status, String code, String detail) {
    this(status, code, detail, Map.of());
  }

  public ApiException(HttpStatus status, String code, String detail, Map<String, Object> context) {
    super(detail);
    this.status = status;
    this.code = code;
    this.context = Map.copyOf(context);
  }

  public HttpStatus status() {
    return status;
  }

  public String code() {
    return code;
  }

  public Map<String, Object> context() {
    return context;
  }
}
