package com.teya;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serial;
import java.io.Serializable;

public class Day1Step1Solution extends ProcessFunction<String, Integer> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private transient ValueState<Integer> currentDialIndex;

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Integer> currentDialIndexDescriptor = new ValueStateDescriptor<>(
                "currentDialIndex", TypeInformation.of(Integer.class));

        currentDialIndex = getRuntimeContext().getState(currentDialIndexDescriptor);
    }

    @Override
    public void processElement(String s, ProcessFunction<String, Integer>.Context context, Collector<Integer> collector) throws Exception {
        if (currentDialIndex.value() == null) {
            currentDialIndex.update(50);
        }

        var direction = s.charAt(0) == 'L' ? -1 : 1;
        var steps = Integer.parseInt(s.substring(1)) * direction;
        var finalIndex = (currentDialIndex.value() + steps) % 100;
        currentDialIndex.update(finalIndex);
        if (finalIndex == 0){
            collector.collect(1);
        }
    }
}