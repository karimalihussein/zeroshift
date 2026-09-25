package io.zeroshift.docs;

import io.zeroshift.docs.Model.ErrorDoc;
import io.zeroshift.docs.Model.Field;
import io.zeroshift.docs.Model.Operation;
import io.zeroshift.docs.Model.Param;
import io.zeroshift.docs.Model.ResponseDoc;
import io.zeroshift.docs.Model.Schema;
import java.util.ArrayList;
import java.util.List;

/** Builds one documented HTTP operation without repeating the record's boilerplate. */
final class Op {
  static final String AUTH =
      "None. ZeroShift is a local lab and does not authenticate callers. That is deliberate, not an unfinished feature.";
  static final List<String> COMMERCE =
      List.of(
          "order-service",
          "payment-service",
          "inventory-service",
          "shipping-service",
          "order-query-service");

  private final String module;
  private final String service;
  private final String method;
  private final String path;
  private final boolean api;
  private String group = "API";
  private String title;
  private String summary = "";
  private String auth = AUTH;
  private final List<Param> headers = new ArrayList<>();
  private final List<Param> pathParams = new ArrayList<>();
  private final List<Param> query = new ArrayList<>();
  private Schema request;
  private final List<ResponseDoc> responses = new ArrayList<>();
  private final List<ErrorDoc> errors = new ArrayList<>();
  private String idempotency = "Not an idempotent HTTP write. A retry can repeat the effect.";
  private String consistency =
      "No consistency token. The response is the service's own state at the time it answers.";
  private List<String> events = List.of();
  private String source;

  private Op(String module, String service, String method, String path, boolean api) {
    this.module = module;
    this.service = service;
    this.method = method;
    this.path = path;
    this.api = api;
    headers.add(
        param(
            "X-Request-Id",
            "string",
            false,
            null,
            "Request header, optional. 1–100 characters from [A-Za-z0-9._:-]. Missing or invalid values are replaced. The same value is echoed on the response and on every error."));
  }

  static Op api(String module, String service, String method, String path) {
    return new Op(module, service, method, path, true);
  }

  static Op view(String module, String method, String path) {
    var op = new Op(module, "migration-lab", method, path, false);
    op.auth = "Browser page. No API authentication.";
    op.idempotency = "Not an API call.";
    return op;
  }

  Op group(String group) {
    this.group = group;
    return this;
  }

  Op title(String title) {
    this.title = title;
    return this;
  }

  Op summary(String summary) {
    this.summary = summary;
    return this;
  }

  Op header(String name, boolean required, String description) {
    headers.add(param(name, "string", required, null, description));
    return this;
  }

  Op path(String name, String type, String description) {
    pathParams.add(param(name, type, true, null, description));
    return this;
  }

  Op query(String name, String type, boolean required, String defaultValue, String description) {
    query.add(param(name, type, required, defaultValue, description));
    return this;
  }

  Op body(Schema schema) {
    this.request = schema;
    return this;
  }

  Op response(int status, String description, Schema schema) {
    var type = status == 204 ? null : "application/json";
    responses.add(new ResponseDoc(status, description, type, schema));
    return this;
  }

  Op error(int status, String code, String when) {
    errors.add(new ErrorDoc(status, code, when));
    return this;
  }

  Op idempotency(String text) {
    this.idempotency = text;
    return this;
  }

  Op consistency(String text) {
    this.consistency = text;
    return this;
  }

  Op events(String... types) {
    this.events = List.of(types);
    return this;
  }

  Op source(String source) {
    this.source = source;
    return this;
  }

  Operation done(String id) {
    boolean placeOrder =
        api && method.equals("POST") && "/orders".equals(path) && "order-service".equals(service);
    boolean tryIt = api && (method.equals("GET") || placeOrder);
    var hosts =
        "platform".equals(module)
            ? COMMERCE
            : "migration-lab".equals(module) ? List.<String>of() : List.of(service);
    return new Operation(
        id,
        module,
        service,
        hosts,
        group,
        title == null ? method + " " + path : title,
        method,
        path,
        api,
        tryIt,
        summary,
        auth,
        List.copyOf(headers),
        List.copyOf(pathParams),
        List.copyOf(query),
        request,
        List.copyOf(responses),
        List.copyOf(errors),
        idempotency,
        consistency,
        events,
        source);
  }

  static Field field(String name, String type, boolean required, String description) {
    return new Field(name, type, required, description);
  }

  static Schema schema(String record, String source, String example, Field... fields) {
    return new Schema(record, source, List.of(fields), example);
  }

  static Schema none() {
    return null;
  }

  private static Param param(
      String name, String type, boolean required, String defaultValue, String description) {
    return new Param(name, type, required, defaultValue, description);
  }
}
