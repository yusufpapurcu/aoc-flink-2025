package com.yusufpapurcu.day5;

import com.yusufpapurcu.day3.BankMaxJoltageMapper;
import com.yusufpapurcu.day3.BatteryMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputPath = args.length > 0 ? args[0] : "input/day5.txt";

        FileSource<String> source = FileSource
            .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
            .build();

        DataStream<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "file-source"
        );

        // Part 1
        stream
                .process(new SolutionPart1())
                .filter(new DeduplicateAvailableIds())
                .map(_ -> 1L)
                .setParallelism(1) // needed for ordered processing
                .windowAll(GlobalWindows.createWithEndOfStreamTrigger())
                .sum(0)
                .print("Day 5 - Part 1 Result");

        // Part 2
        DataStream<String> stream2 = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "file-source-2"
        );

        stream2
                .process(new SolutionPart2())
                .setParallelism(1) // needed for ordered processing
                .print("Day 5 - Part 2 Result");

        env.execute("File to Console Job");
    }
}
