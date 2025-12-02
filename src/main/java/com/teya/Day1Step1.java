package com.teya;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

public class Day1Step1 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputPath = args.length > 0 ? args[0] : "day1.txt";

        FileSource<String> source = FileSource
            .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
            .build();

        DataStream<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "file-source"
        );

        stream
                .keyBy(s -> "")
                .process(new Day1Step1Solution())
                .windowAll(GlobalWindows.createWithEndOfStreamTrigger())
                .aggregate(new AverageAggregate())
                .print();

        env.execute("File to Console Job");
    }
}
