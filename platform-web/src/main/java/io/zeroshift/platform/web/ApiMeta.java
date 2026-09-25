package io.zeroshift.platform.web;

import com.fasterxml.jackson.annotation.JsonInclude;

/** Facts about a list: how many items, the limit asked for, and whether more exist beyond it. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiMeta(int count, Integer limit, Boolean hasMore) {}
