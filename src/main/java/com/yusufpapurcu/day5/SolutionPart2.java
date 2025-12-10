package com.yusufpapurcu.day5;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SolutionPart2 extends ProcessFunction<String, Long> {
    private List<Tuple2<Long, Long>> ranges = new ArrayList<>();
    private boolean rangesFinalized = false;

    @Override
    public void processElement(String s, ProcessFunction<String, Long>.Context context, Collector<Long> collector) throws Exception {
        // If we've already processed ranges, skip remaining lines
        if (rangesFinalized) {
            return;
        }

        // Empty line or number (not a range) means we're done with ranges
        if (s.isEmpty() || !s.contains("-")) {
            // Merge overlapping ranges and count total IDs
            long totalCount = countTotalFreshIds();
            collector.collect(totalCount);
            rangesFinalized = true;
            return;
        }

        // Parse range
        var parts = s.split("-");
        var start = Long.parseLong(parts[0]);
        var end = Long.parseLong(parts[1]);
        ranges.add(Tuple2.of(start, end));
    }

    private long countTotalFreshIds() {
        if (ranges.isEmpty()) {
            return 0;
        }

        // Sort ranges by start position
        ranges.sort(Comparator.comparing(r -> r.f0));

        // Merge overlapping ranges
        List<Tuple2<Long, Long>> merged = new ArrayList<>();
        Tuple2<Long, Long> current = ranges.get(0);

        for (int i = 1; i < ranges.size(); i++) {
            Tuple2<Long, Long> next = ranges.get(i);

            // If ranges overlap or are adjacent, merge them
            if (next.f0 <= current.f1 + 1) {
                current = Tuple2.of(current.f0, Math.max(current.f1, next.f1));
            } else {
                // No overlap, add current to merged list and move to next
                merged.add(current);
                current = next;
            }
        }
        // Add the last range
        merged.add(current);

        // Count total IDs in all merged ranges
        long count = 0;
        for (Tuple2<Long, Long> range : merged) {
            count += (range.f1 - range.f0 + 1);
        }

        return count;
    }
}
