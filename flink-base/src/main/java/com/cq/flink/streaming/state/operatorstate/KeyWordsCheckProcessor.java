package com.cq.flink.streaming.state.operatorstate;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author chenquan
 * @Description
 * @Date 2022-04-18 23:07
 **/

public class KeyWordsCheckProcessor extends BroadcastProcessFunction<String, Tuple2<String, String>, String> {
    MapStateDescriptor<String, String> descriptor =   new MapStateDescriptor<String,String>("ControlStream",String.class,String.class);

    @Override
    public void processElement(String value, BroadcastProcessFunction<String, Tuple2<String, String>, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
        //TODO 从 broadcast state 中拿到控制信息  com.cq.flink
        String keywords = readOnlyContext.getBroadcastState(descriptor).get("key");
        //TODO 获取符合条件的单词
        if (value.contains(keywords)) {
            collector.collect(value);
        }
    }

    @Override
    public void processBroadcastElement(Tuple2<String, String> value, BroadcastProcessFunction<String, Tuple2<String, String>, String>.Context context, Collector<String> collector) throws Exception {
        //TODO 将接收到的控制数据放到 broadcast state 中
        context.getBroadcastState(descriptor).put(value.f0, value.f1);
        System.out.println(Thread.currentThread().getName() + " 接收到控制信息 >>" + value);
    }
}
