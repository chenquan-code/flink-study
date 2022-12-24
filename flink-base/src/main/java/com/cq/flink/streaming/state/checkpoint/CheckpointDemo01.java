package com.cq.flink.streaming.state.checkpoint;

import com.cq.flink.util.FlinkUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @Author chenquan
 * @Description 测试Flink Checkpoint功能
 * @Date 2022-04-17 1:16
 **/

public class CheckpointDemo01 {

    public static void main(String[] args) throws Exception {

        //TODO 使用FlinkUtil获取流式编程入口
        StreamExecutionEnvironment env = FlinkUtil.getStreamExecutionEnvironment(true);
        //TODO 设置checkpoints地址
        System.setProperty("HADOOP_USER_NAME", "root");
        env.setStateBackend(new FsStateBackend("hdfs://node1:9000/com.cq.flink/checkpoint"));
        //TODO 启用Checkpoint并设置时间，这个时间决定了发生checkpoint的间隔，也决定了任务的时效性，通常根据业务的需求来设定，假设业务能容忍的延迟为五分钟，那就设置为五分钟以内
        env.enableCheckpointing(5000);
        //TODO 设置checkpoint语义为：仅一次处理
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //TODO 两次checkpoint的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        //TODO 最多几个checkpoints同时进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //TODO checkpoint超时的时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //TODO 设置重试策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));
        //TODO Cancel程序的时候保存checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //TODO 读取Socket数据源
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = line.split(",");
                for (String word : split) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(tuple -> tuple.f0).sum(1);
        wordCount.print();

        env.execute("CheckpointDemo01");


    }



}
