package io.zeroshift.platform.web;

import java.io.IOException;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

/** For RestClient callers: forwards the current request's id, so one id spans the whole hop. */
public class RequestIdPropagation implements ClientHttpRequestInterceptor {
  @Override
  public ClientHttpResponse intercept(
      HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
    var id = RequestContext.requestId();
    if (id != null && !request.getHeaders().containsHeader(ApiHeaders.REQUEST_ID))
      request.getHeaders().set(ApiHeaders.REQUEST_ID, id);
    return execution.execute(request, body);
  }
}
