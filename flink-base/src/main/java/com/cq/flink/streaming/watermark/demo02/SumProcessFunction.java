package com.cq.flink.streaming.watermark.demo02;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author chenqzz@163.com
 * @Date 2022-05-03 2:20
 * @Description
 **/

public class  SumProcessFunction extends ProcessWindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>, String, TimeWindow> {

    FastDateFormat dataformat=FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void process(String key,Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
        System.out.println("当前系统时间："+dataformat.format(System.currentTimeMillis()));
        System.out.println("窗口处理时间："+dataformat.format(context.currentProcessingTime()));

        System.out.println("窗口开始时间："+dataformat.format(context.window().getStart()));
        System.out.println("窗口结束时间："+dataformat.format(context.window().getEnd()));

        System.out.println("=====================================================");

        //实现了sum的效果
        int count=0;
        for (Tuple2<String,Integer> e:iterable){
            count++;
        }
        collector.collect(Tuple2.of(key,count));
    }
}