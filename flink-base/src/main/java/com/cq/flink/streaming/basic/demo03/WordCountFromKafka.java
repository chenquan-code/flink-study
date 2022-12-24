package com.cq.flink.streaming.basic.demo03;

import com.cq.flink.streaming.basic.demo02.WordAndOne;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author chenquan
 * @Description 消费kafka做词频统计
 * @Date 2022-04-16 18:39
 **/

public class WordCountFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        String topic = "test1";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","node1:9092");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id","flink_consumer");

        //TODO 创建Kafka消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        //TODO 添加自定义Source
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        SingleOutputStreamOperator<WordAndOne> wordAndOne = kafkaData.flatMap(new FlatMapFunction<String, WordAndOne>() {
                    @Override
                    public void flatMap(String line, Collector<WordAndOne> collector) throws Exception {
                        String[] split = line.split(",");
                        for (String word : split) {
                            collector.collect(new WordAndOne(word, 1));
                        }
                    }
                }).keyBy(WordAndOne::getWord)
                .sum("one");

        wordAndOne.map(WordAndOne::toString).print();

        env.execute();


    }
}
