package com.yusufpapurcu.day2;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class SplitItemsFlatMap extends RichFlatMapFunction<String, String> {
    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        Arrays.stream(s.split(",")).forEach(collector::collect);
    }
}
