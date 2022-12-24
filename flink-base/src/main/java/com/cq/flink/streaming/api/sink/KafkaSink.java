package com.cq.flink.streaming.api.sink;

import com.cq.flink.streaming.api.source.JsonSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Author chenquan
 * @Description kafka生产者程序，用于造数据
 * @Date 2022-04-16 20:42
 **/

public class KafkaSink {
    public static void main(String[] args) throws Exception {
        //TODO 获取流式程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.addSource(new JsonSource()).setParallelism(3);
        //TODO 在控制台打印数据
//        dataStreamSource.print("source");
        //TODO Kafka
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 5);
        //TODO  发送到kafka
        // kafka-topics.sh --zookeeper node1:2181 --create --replication-factor 3 --partitions 1 --topic ods.flink_on_hudi_t1
        // kafka-console-consumer.sh --bootstrap-server node1:9092 --topic ods.flink_on_hudi_t1 --from-beginning
        dataStreamSource.addSink(new FlinkKafkaProducer<String>(
                "ods.flink_on_hudi_t1",
                new SimpleStringSchema(),
                properties)
        );
        env.execute("UserDefineKafkaSource");
    }
}
