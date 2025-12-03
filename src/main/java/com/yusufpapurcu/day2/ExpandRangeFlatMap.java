package com.yusufpapurcu.day2;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class ExpandRangeFlatMap extends RichFlatMapFunction<String, String> {
    @Override
    public void flatMap(String s, Collector<String> collector) {
        var start = Long.parseLong(s.substring(0, s.indexOf('-')));
        var end = Long.parseLong(s.substring(s.indexOf('-') + 1));

        for (long i = start; i <= end; i++) {
            collector.collect(String.valueOf(i));
        }
    }
}
