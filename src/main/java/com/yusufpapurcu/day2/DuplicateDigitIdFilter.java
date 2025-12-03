package com.yusufpapurcu.day2;

import org.apache.flink.api.common.functions.RichFilterFunction;

public class DuplicateDigitIdFilter extends RichFilterFunction<String> {
    @Override
    public boolean filter(String id) {
        if (id.length() % 2 != 0) {
            return false;
        }

        var firstHalf = id.substring(0, id.length() / 2);
        var secondHalf = id.substring(id.length() / 2);

        return firstHalf.equals(secondHalf);
    }
}
