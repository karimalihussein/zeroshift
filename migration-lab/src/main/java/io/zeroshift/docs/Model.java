package io.zeroshift.docs;

import java.util.List;

/** The developer portal's wire model. Serialized once at {@code /api/docs/catalog}. */
public final class Model {
  private Model() {}

  public record Portal(
      String name,
      String version,
      String tagline,
      List<NavGroup> navigation,
      List<Page> pages,
      List<Operation> operations,
      List<EventDoc> events,
      List<TopicDoc> topics,
      List<ServiceDoc> services,
      List<CodeDoc> errors,
      List<SearchHit> search) {}

  public record NavGroup(String label, List<NavItem> items) {}

  public record NavItem(String label, String href, String method) {}

  public record Page(String slug, String title, String group, String summary, String markdown) {}

  public record SearchHit(String kind, String title, String href, String detail) {}

  public record Field(String name, String type, boolean required, String description) {}

  public record Param(
      String name, String type, boolean required, String defaultValue, String description) {}

  public record Schema(String record, String source, List<Field> fields, String example) {}

  public record ResponseDoc(int status, String description, String contentType, Schema schema) {}

  public record ErrorDoc(int status, String code, String when) {}

  public record Operation(
      String id,
      String module,
      String service,
      List<String> hosts,
      String group,
      String title,
      String method,
      String path,
      boolean api,
      boolean tryIt,
      String summary,
      String auth,
      List<Param> headers,
      List<Param> pathParams,
      List<Param> query,
      Schema request,
      List<ResponseDoc> responses,
      List<ErrorDoc> errors,
      String idempotency,
      String consistency,
      List<String> events,
      String source) {}

  public record EventDoc(
      String type,
      String family,
      String topic,
      int version,
      String javaType,
      List<Field> payload,
      String example,
      String envelope,
      List<String> producers,
      List<String> consumers,
      String partitionKey,
      String ordering,
      String idempotency,
      String retry,
      String dlq,
      List<String> upcasters,
      List<String> headers) {}

  public record TopicDoc(
      String name,
      Integer partitions,
      String replicationFactor,
      String retention,
      String cleanup,
      List<String> configs,
      List<String> producers,
      List<String> consumerGroups,
      String messageKey,
      String deadLetter,
      List<String> eventTypes,
      String notes) {}

  public record ServiceDoc(
      String id,
      String name,
      String purpose,
      int port,
      String replica,
      String database,
      String health,
      String module,
      List<String> rest,
      List<String> produces,
      List<String> consumes,
      List<String> dependsOn,
      String observability) {}

  public record CodeDoc(String code, int status, String scope, String when) {}
}
