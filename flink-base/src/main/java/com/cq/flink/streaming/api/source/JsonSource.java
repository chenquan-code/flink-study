package com.cq.flink.streaming.api.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author chenquan
 * @Description 自定义数据源
 * @Date 2022-04-16 20:45
 **/

public class JsonSource implements ParallelSourceFunction<String> {
    public static AtomicInteger count = new AtomicInteger();
    String sourceStr = "{\"id\":\"1\",\"name\":\"zs\",\"sex\":1,\"age\":1}";

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (count.incrementAndGet() <= 10000000) {
            sourceContext.collect(sourceStr);
            // 每隔 300ms 钟发送一条数据
//            Thread.sleep(300);
        }
    }

    @Override
    public void cancel() {

    }
}
