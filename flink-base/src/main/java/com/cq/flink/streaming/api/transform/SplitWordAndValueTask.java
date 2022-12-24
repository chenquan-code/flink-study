package com.cq.flink.streaming.api.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author chenquan
 * @Description 将单词转换为（word,值）
 * @Date 2022-04-17 16:57
 **/

public class SplitWordAndValueTask implements FlatMapFunction<String, Tuple2<String,Integer>> {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] split = line.split(",");
        collector.collect(Tuple2.of(split[0],Integer.valueOf(split[1])));
    }
}
