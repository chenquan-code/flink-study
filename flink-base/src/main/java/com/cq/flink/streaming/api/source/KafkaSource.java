package com.cq.flink.streaming.api.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author chenqzz@163.com
 * @Date 2022-05-17 1:48
 * @Description
 **/

public class KafkaSource {
    public static void main(String[] args) throws Exception {
        /**
         * 步骤一：获取执行环境
         *
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //我们是从Kafka里面读取数据，所以这儿就是topic有多少个partition，那么就设置几个并行度。
        env.setParallelism(25);
        //设置checkpoint的参数
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String topic="ods.flink_on_hudi_t1";
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers","node1:9092");
        consumerProperties.put("group.id","allTopic_consumer");
        consumerProperties.put("enable.auto.commit", "false");
        consumerProperties.put("auto.offset.reset","earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic,
                new SimpleStringSchema(),
                consumerProperties);

         //TODO 步骤二：获取数据源
        DataStreamSource<String> allData = env.addSource(consumer);
        // TODO 步骤三：打印控制台
        allData.print();

        env.execute("KafkaSource");










    }
}
