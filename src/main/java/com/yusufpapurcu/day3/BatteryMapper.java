package com.yusufpapurcu.day3;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;
import java.util.Set;

public class BatteryMapper extends RichMapFunction<Map<Integer, Set<Integer>>, Long> {

    private final int batterySize;

    public BatteryMapper(int batterySize) {
        this.batterySize = batterySize;
    }

    @Override
    public Long map(Map<Integer, Set<Integer>> batteries) throws Exception {
        var lastBatteryIndex = -1;
        var result = 0L;
        var totalLength = batteries.values().stream().mapToInt(Set::size).sum() - 1;
        for (int i = batterySize - 1; i >= 0; i--) {
            var res = findNextBattery(batteries, lastBatteryIndex, totalLength - i);
            lastBatteryIndex = res.f1;
            result += res.f0 * (long) Math.pow(10, i);
        }

        return result;
    }

    private Tuple2<Long, Integer> findNextBattery(Map<Integer, Set<Integer>> batteries, Integer lastBatteryIndex, Integer spaceToLeft) {
        for (int i = 9; i >= 0; i--) {
            var joltage = batteries.get(i);
            if (joltage == null || joltage.isEmpty()){
                continue;
            }

            if (joltage.stream().anyMatch(x -> x > lastBatteryIndex && x <= spaceToLeft)){
                var index = joltage.stream().filter(x -> x > lastBatteryIndex).sorted().toList().getFirst();
                if (!joltage.remove(index)){
                    throw new RuntimeException("Could not remove used battery index: " + index + " for joltage: " + i);
                }

                return Tuple2.of((long) i, index);
            }
        }

        throw new IllegalStateException("No battery found for index: " + lastBatteryIndex);
    }
}
