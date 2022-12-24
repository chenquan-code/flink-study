package com.cq.flink.streaming.watermark.demo02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author chenqzz@163.com
 * @Date 2022-05-03 2:03
 * @Description 每隔5秒统计最近10秒的订单数， 使用自定义处理逻辑
 **/

public class CustomWindowWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = line.split(",");
                for (String word : split) {
                    collector.collect(Tuple2.of(word,1));
                }
            }
        }).keyBy(tuple -> tuple.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new SumProcessFunction())
                .print().setParallelism(1);

        env.execute("CustomWindowWordCount");
    }
}
