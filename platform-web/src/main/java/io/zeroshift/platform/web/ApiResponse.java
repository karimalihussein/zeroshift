package io.zeroshift.platform.web;

import java.util.List;

/**
 * The envelope for collections: {@code {"data": [...], "meta": {...}}}. Single resources and
 * command results are returned as themselves; wrapping them would add nothing HTTP does not say.
 */
public record ApiResponse<T>(T data, ApiMeta meta) {
  /** A complete list: every item there is. */
  public static <T> ApiResponse<List<T>> list(List<T> items) {
    return new ApiResponse<>(List.copyOf(items), new ApiMeta(items.size(), null, null));
  }

  /**
   * A limited list. Pass up to {@code limit + 1} items: the extra one only proves that more exist
   * and is dropped.
   */
  public static <T> ApiResponse<List<T>> page(List<T> fetched, int limit) {
    boolean more = fetched.size() > limit;
    var items = more ? fetched.subList(0, limit) : fetched;
    return new ApiResponse<>(List.copyOf(items), new ApiMeta(items.size(), limit, more));
  }
}
