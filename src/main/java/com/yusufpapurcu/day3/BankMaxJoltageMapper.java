package com.yusufpapurcu.day3;

import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BankMaxJoltageMapper extends RichMapFunction<String, Map<Integer, Set<Integer>>> {
    @Override
    public Map<Integer, Set<Integer>> map(String s) throws Exception {
        var out = new HashMap<Integer, Set<Integer>>();
        for (int i = 0; i < s.length(); i++) {
            var joltage = Integer.parseInt(String.valueOf(s.charAt(i)));
            out.computeIfAbsent(joltage, k -> new HashSet<>()).add(i);
        }
        return out;
    }
}
