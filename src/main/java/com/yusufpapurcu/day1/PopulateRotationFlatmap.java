package com.yusufpapurcu.day1;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class PopulateRotationFlatmap extends RichFlatMapFunction<String, String> {
    @Override
    public void flatMap(String s, Collector<String> collector) {
        var direction = s.charAt(0);
        var steps = Integer.parseInt(s.substring(1));

        for (int i = 0; i < steps; i++) {
            collector.collect(String.valueOf(direction));
        }
    }
}
