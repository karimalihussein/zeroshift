package io.zeroshift.order.infrastructure;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.zeroshift.order.application.Catalog;
import io.zeroshift.order.application.CatalogUnavailable;
import io.zeroshift.platform.web.RequestIdPropagation;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

/**
 * The catalog over HTTP: {@code GET /products?sku=…} on inventory-service, one call per placement.
 * A synchronous dependency on purpose (ADR 021): the timeouts are short, so an order placed while
 * inventory is down fails fast with 503 instead of holding the client and a thread.
 */
public final class HttpCatalog implements Catalog {
  @JsonIgnoreProperties(ignoreUnknown = true)
  record Page(List<Item> data) {}

  /** The fields of inventory's product that an order copies; the rest is ignored. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  record Item(
      UUID id, String sku, String name, BigDecimal price, String currency, boolean active) {}

  private final RestClient http;

  public HttpCatalog(URI baseUrl) {
    var factory =
        new JdkClientHttpRequestFactory(
            HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build());
    factory.setReadTimeout(Duration.ofSeconds(2));
    http =
        RestClient.builder()
            .baseUrl(baseUrl.toString())
            .requestFactory(factory)
            .requestInterceptor(new RequestIdPropagation())
            .build();
  }

  @Override
  public List<Product> find(Collection<String> skus) {
    if (skus.isEmpty()) return List.of();
    try {
      var page =
          http.get()
              .uri(b -> b.path("/products").queryParam("sku", skus.toArray()).build())
              .retrieve()
              .body(Page.class);
      if (page == null || page.data() == null) return List.of();
      return page.data().stream()
          .map(i -> new Product(i.id(), i.sku(), i.name(), i.price(), i.currency(), i.active()))
          .toList();
    } catch (RestClientResponseException e) {
      if (e.getStatusCode().isSameCodeAs(HttpStatus.NOT_FOUND)) return List.of();
      throw new CatalogUnavailable("Catalog answered " + e.getStatusCode().value(), e);
    } catch (RestClientException e) {
      throw new CatalogUnavailable("Catalog unreachable: " + e.getMessage(), e);
    }
  }
}
