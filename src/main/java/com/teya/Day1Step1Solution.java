package com.teya;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class Day1Step1Solution extends RichFlatMapFunction<String, Integer> {
    private int currentDialIndex = 50;

    @Override
    public void flatMap(String s, Collector<Integer> collector) {
        var direction = s.charAt(0) == 'L' ? -1 : 1;
        var steps = Integer.parseInt(s.substring(1)) * direction;
        currentDialIndex = (currentDialIndex + steps) % 100;

        if (currentDialIndex == 0) {
            collector.collect(1);
        }
    }
}
