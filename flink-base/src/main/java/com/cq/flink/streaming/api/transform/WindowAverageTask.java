package com.cq.flink.streaming.api.transform;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

/**
 * @Author chenquan
 * @Description 开窗求平均值
 *  MapState<K, V> ：这个状态为每一个 key 保存一个 Map 集合
 *      put() 将对应的 key 的键值对放到状态中
 *      values() 拿到 MapState 中所有的 value
 *      clear() 清除状态
 * @Date 2022-04-17 17:01
 **/

public class WindowAverageTask extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Double>> {

    private MapState<String,Integer> mapState;

    /**
     *  open方法只会被执行一次
     * @param parameters
     */
    @Override
    public void open(Configuration parameters) {
        //TODO 注册状态
        MapStateDescriptor<String, Integer> averageDescriptor = new MapStateDescriptor<>("average", String.class, Integer.class);
        //TODO 读取状态数据
        mapState = getRuntimeContext().getMapState(averageDescriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Integer> input, Collector<Tuple2<String, Double>> collector) throws Exception {

        mapState.put(UUID.randomUUID().toString(),input.f1);
        //TODO 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        ArrayList<Integer> allElements = Lists.newArrayList(mapState.values());
        if (allElements.size() == 3){
            int count = 0;
            int sum = 0;
            for (Integer value : allElements){
                count++;
                sum += value;
            }
            double avg = (double) sum / count;
            collector.collect(Tuple2.of(input.f0,avg));
            //TODO 清除状态
            mapState.clear();
        }


    }
}
