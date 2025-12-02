package com.yusufpapurcu.day1;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class RotateFlatmap extends RichFlatMapFunction<String, Integer> {
    private int currentDialIndex = 50;

    @Override
    public void flatMap(String s, Collector<Integer> collector) {
        var direction = s.charAt(0) == 'L' ? -1 : 1;
        currentDialIndex = (currentDialIndex + direction) % 100;

        if (currentDialIndex == 0) {
            collector.collect(1);
        }
    }
}
