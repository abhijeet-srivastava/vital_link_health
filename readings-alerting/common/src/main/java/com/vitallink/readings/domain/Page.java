package com.vitallink.readings.domain;

import java.util.List;

/**
 * One keyset page. Adapters fetch limit + 1 rows: hasMore says whether a next page exists, and the caller builds the
 * next cursor from the last item, so a page never needs a count or an OFFSET.
 */
public record Page<T>(List<T> items, boolean hasMore) {

    public Page {
        items = List.copyOf(items);
    }

    public static <T> Page<T> of(List<T> rows, int limit) {
        return rows.size() > limit ? new Page<>(rows.subList(0, limit), true) : new Page<>(rows, false);
    }

    public T last() {
        return items.get(items.size() - 1);
    }
}
