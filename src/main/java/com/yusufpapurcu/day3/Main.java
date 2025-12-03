package com.yusufpapurcu.day3;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputPath = args.length > 0 ? args[0] : "day3.txt";

        FileSource<String> source = FileSource
            .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
            .build();

        DataStream<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "file-source"
        );

        stream
                .map(new BankMaxJoltageMapper())
                .map(new BatteryMapper(2))
                .windowAll(GlobalWindows.createWithEndOfStreamTrigger())
                .sum(0)
                .print("Day 3 - Part 1 Result");

        stream
                .map(new BankMaxJoltageMapper())
                .map(new BatteryMapper(12))
                .windowAll(GlobalWindows.createWithEndOfStreamTrigger())
                .sum(0)
                .print("Day 3 - Part 2 Result");

        env.execute("File to Console Job");
    }
}
