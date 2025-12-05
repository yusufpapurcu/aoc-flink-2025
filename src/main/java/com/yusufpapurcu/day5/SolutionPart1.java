package com.yusufpapurcu.day5;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Set;

public class SolutionPart1 extends ProcessFunction<String, Long> {
    private List<Tuple2<Long, Long>> ranges = new java.util.ArrayList<>();

    @Override
    public void processElement(String s, ProcessFunction<String, Long>.Context context, Collector<Long> collector) throws Exception {
        if (s.isEmpty()) {
            return;
        }

        if (s.contains("-")) {
            var parts = s.split("-");
            var start = Long.parseLong(parts[0]);
            var end = Long.parseLong(parts[1]);

            ranges.add(Tuple2.of(start, end));
        } else {
            var number = Long.parseLong(s);
            var inAnyRange = ranges.stream().anyMatch(range -> number >= range.f0 && number <= range.f1);
            if (inAnyRange) {
                collector.collect(number);
            }
        }
    }
}
