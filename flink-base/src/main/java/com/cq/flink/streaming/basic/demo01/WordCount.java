package com.cq.flink.streaming.basic.demo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author chenquan
 * @Description 从socket实时读取数据统计word count
 * 将匿名内部类提取出来
 * @Date 2022-04-16 0:39
 **/

public class WordCount {

    public static void main(String[] args) throws Exception {
        //TODO 获取流式程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO window下使打开socket 程序： nc -lp 8888
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = dataStream
                .flatMap(new SplitTask())
                .keyBy(tuple -> tuple.f0)
                .sum(1);
        //TODO 输出
        wordCount.print();
        //TODO 启动任务
        env.execute();


    }

    /**
     * 单词切割逻辑，将匿名内部类提取出来
     */
    static class SplitTask implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] split = line.split(",");
            for (String word : split){
                collector.collect(Tuple2.of(word,1));
            }
        }
    }



}
