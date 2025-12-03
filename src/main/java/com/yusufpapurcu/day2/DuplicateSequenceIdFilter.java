package com.yusufpapurcu.day2;

import org.apache.flink.api.common.functions.RichFilterFunction;

public class DuplicateSequenceIdFilter extends RichFilterFunction<String> {
    @Override
    public boolean filter(String id) {
        var idLength = id.length();
        for (int i = 1; i < idLength; i++) {
            var sequence = id.substring(0, i);
            var repeat = idLength / i;

            if (sequence.repeat(repeat).equals(id)) {
                return true;
            }
        }

        return false;
    }
}
