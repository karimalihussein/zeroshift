package io.zeroshift.docs;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeroshift.contracts.Contracts;
import io.zeroshift.docs.Model.Schema;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class PortalCatalogTest {
  private static final Pattern MAPPING =
      Pattern.compile("@(Get|Post|Put|Delete|Patch)Mapping(?:\\(([^)]*)\\))?");
  private static final Pattern CODE =
      Pattern.compile("HttpStatus\\.[A-Z_]+,\\s*\"([A-Z][A-Z0-9_]+)\"");
  private static final Pattern CONSTANT =
      Pattern.compile("public static final String ([A-Z][A-Z0-9_]+) = \"\\1\"");

  private final JsonMapper json = JsonMapper.builder().build();

  @Test
  void catalogMatchesTheRepository() throws IOException {
    var portal = PortalCatalog.build();
    var root = Path.of("").toAbsolutePath().getParent();

    assertThat(portal.events())
        .extracting(event -> event.type())
        .containsExactlyInAnyOrderElementsOf(
            StreamSupport.stream(Contracts.all().spliterator(), false).map(c -> c.type()).toList());
    for (var event : portal.events()) {
      json.readTree(event.example());
      json.readTree(event.envelope());
      assertThat(event.version()).isEqualTo(Contracts.of(event.type()).version());
      assertThat(event.topic()).isEqualTo(Contracts.of(event.type()).topic());
    }

    var script = Files.readString(root.resolve("infra/kafka/topics.sh"));
    var declared = TopicScript.parse(script).stream().map(TopicScript.Declared::name).toList();
    assertThat(portal.topics()).extracting(topic -> topic.name()).containsAll(declared);
    assertThat(TopicScript.replicationFactor(script)).isEqualTo(1);
    assertThat(
            PortalCatalog.outboxDatabases(
                Files.readString(root.resolve("infra/debezium/register.sh"))))
        .containsExactly("orders", "payments", "inventory", "shipping");

    var found = mappings(root);
    var documented =
        portal.operations().stream()
            .map(op -> op.module() + " " + op.method() + " " + op.path())
            .toList();
    assertThat(documented).containsExactlyInAnyOrderElementsOf(found);

    for (var operation : portal.operations()) {
      boolean place =
          operation.method().equals("POST")
              && operation.path().equals("/orders")
              && operation.service().equals("order-service");
      assertThat(operation.tryIt())
          .isEqualTo(operation.api() && (operation.method().equals("GET") || place));
      if (operation.request() != null) checkSchema(root, operation.request());
      for (var response : operation.responses())
        if (response.schema() != null) checkSchema(root, response.schema());
    }

    assertThat(codes(root))
        .containsExactlyInAnyOrderElementsOf(
            portal.errors().stream().map(error -> error.code()).toList());

    var hrefs = new HashSet<String>();
    hrefs.add("/docs/services");
    hrefs.add("/docs/events");
    hrefs.add("/docs/kafka");
    hrefs.add("/docs/kafka/topics");
    hrefs.add("/docs/kafka/groups");
    hrefs.add("/docs/kafka/outbox");
    hrefs.add("/docs/kafka/dead-letters");
    portal.pages().forEach(page -> hrefs.add("/docs/" + page.slug()));
    portal.services().forEach(service -> hrefs.add("/docs/services/" + service.id()));
    portal.operations().stream()
        .filter(op -> op.api())
        .forEach(op -> hrefs.add("/docs/api/" + op.id()));
    portal.events().forEach(event -> hrefs.add("/docs/events/" + PortalCatalog.slug(event.type())));
    portal.navigation().stream()
        .flatMap(group -> group.items().stream())
        .map(item -> item.href().split("#")[0])
        .forEach(href -> assertThat(hrefs).as(href).contains(href));
    assertThat(portal.search())
        .anyMatch(hit -> hit.title().equals("OrderPlaced") && hit.kind().equals("Event"));
  }

  @Test
  void proxyRefusesOperationsThatAreNotMarkedSafe() {
    var portal = PortalCatalog.build();
    var crash =
        portal.operations().stream()
            .filter(op -> op.id().equals("platform-post-crash"))
            .findFirst()
            .orElseThrow();
    assertThat(crash.tryIt()).isFalse();
    var place =
        portal.operations().stream()
            .filter(op -> op.id().equals("order-post-orders"))
            .findFirst()
            .orElseThrow();
    assertThat(place.tryIt()).isTrue();
  }

  private static void checkSchema(Path root, Schema schema) throws IOException {
    if (schema.record() == null || schema.source() == null) return;
    var source = Files.readString(root.resolve(schema.source()));
    var components = components(source, schema.record());
    var names =
        schema.fields().stream().map(field -> field.name().split("\\.")[0]).distinct().toList();
    if (schema.fields().stream().noneMatch(field -> field.name().contains(".")))
      assertThat(names).as(schema.record()).containsExactlyElementsOf(components);
    else assertThat(components).as(schema.record()).containsAll(names);
  }

  private static List<String> components(String source, String record) {
    var needle = "record " + record;
    var start = source.indexOf(needle);
    if (start < 0) throw new AssertionError("No record " + record);
    int open = source.indexOf('(', start + needle.length());
    if (open < 0) throw new AssertionError("No record header for " + record);
    int depth = 1;
    var body = new StringBuilder();
    for (int i = open + 1; i < source.length() && depth > 0; i++) {
      char c = source.charAt(i);
      if (c == '(') depth++;
      else if (c == ')') depth--;
      if (depth > 0) body.append(c);
    }
    var plain = stripAnnotations(body.toString());
    var names = new ArrayList<String>();
    for (var part : splitTop(plain)) {
      var tokens = part.trim().split("\\s+");
      if (tokens.length > 0 && !tokens[tokens.length - 1].isBlank())
        names.add(tokens[tokens.length - 1]);
    }
    return names;
  }

  private static List<String> splitTop(String text) {
    var parts = new ArrayList<String>();
    int depth = 0;
    var current = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '<') depth++;
      else if (c == '>') depth--;
      else if (c == ',' && depth == 0) {
        parts.add(current.toString());
        current.setLength(0);
        continue;
      }
      current.append(c);
    }
    if (!current.toString().isBlank()) parts.add(current.toString());
    return parts;
  }

  private static String stripAnnotations(String text) {
    var out = new StringBuilder();
    for (int i = 0; i < text.length(); ) {
      if (text.charAt(i) == '@') {
        i++;
        while (i < text.length() && Character.isJavaIdentifierPart(text.charAt(i))) i++;
        if (i < text.length() && text.charAt(i) == '(') {
          int depth = 1;
          i++;
          while (i < text.length() && depth > 0) {
            if (text.charAt(i) == '(') depth++;
            else if (text.charAt(i) == ')') depth--;
            i++;
          }
        }
      } else out.append(text.charAt(i++));
    }
    return out.toString();
  }

  private static List<String> mappings(Path root) throws IOException {
    var modules =
        List.of(
            "order-service",
            "payment-service",
            "inventory-service",
            "shipping-service",
            "order-query-service",
            "platform",
            "migration-lab");
    var found = new ArrayList<String>();
    for (var module : modules) {
      try (var files = Files.walk(root.resolve(module).resolve("src/main/java"))) {
        files
            .filter(path -> path.toString().endsWith(".java"))
            .forEach(path -> found.addAll(mappings(module, path)));
      }
    }
    return found;
  }

  private static List<String> mappings(String module, Path path) {
    String source;
    try {
      source = Files.readString(path);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    if (source.contains("package io.zeroshift.docs;")) return List.of();
    if (!source.contains("Mapping")) return List.of();
    var classHeader = source.substring(0, Math.max(source.indexOf(" class "), 0));
    var base = pathOf(classHeader);
    var found = new ArrayList<String>();
    var matcher = MAPPING.matcher(source);
    while (matcher.find()) {
      if (matcher.start() < classHeader.length()) continue;
      var method = matcher.group(1).toUpperCase();
      var pathValue = join(base, pathOf(matcher.group(2) == null ? "" : matcher.group(2)));
      found.add(module + " " + method + " " + pathValue);
    }
    return found;
  }

  private static String pathOf(String annotation) {
    if (annotation == null || annotation.isBlank()) return "";
    var quoted = Pattern.compile("\"([^\"]*)\"").matcher(annotation);
    if (quoted.find()) return quoted.group(1);
    return "";
  }

  private static String join(String base, String child) {
    if (base.isEmpty()) return child.isEmpty() ? "/" : child;
    if (child.isEmpty()) return base;
    return base.endsWith("/") ? base + child.substring(1) : base + child;
  }

  private static Set<String> codes(Path root) throws IOException {
    var codes = new HashSet<String>();
    try (var files = Files.walk(root)) {
      files
          .filter(
              path ->
                  path.toString().contains("/src/main/java/") && path.toString().endsWith(".java"))
          .filter(path -> !path.toString().contains("/generated/"))
          // Git worktrees of other branches (.claude/worktrees/…) are not this repository's code.
          .filter(path -> !root.relativize(path).startsWith(".claude"))
          .forEach(
              path -> {
                try {
                  var source = Files.readString(path);
                  var http = CODE.matcher(source);
                  while (http.find()) codes.add(http.group(1));
                  var constants = CONSTANT.matcher(source);
                  while (constants.find()) codes.add(constants.group(1));
                } catch (IOException e) {
                  throw new IllegalStateException(e);
                }
              });
    }
    return codes;
  }
}
