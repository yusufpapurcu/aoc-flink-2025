package com.yusufpapurcu.day5;

import org.apache.flink.api.common.functions.RichFilterFunction;

import java.util.HashSet;
import java.util.Set;

public class DeduplicateAvailableIds extends RichFilterFunction<Long> {
    private final Set<Long> seenIds = new HashSet<>();

    @Override
    public boolean filter(Long s) throws Exception {
        if (seenIds.contains(s)) {
            return false;
        } else {
            seenIds.add(s);
            return true;
        }
    }
}
