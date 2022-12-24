package com.cq.flink.postgres;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

/**
 * @Author chenquan@banggood.com
 * @Description
 * @Date 2022/6/15 15:24
 **/

public class PostgreSQLSourceExample {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
//        properties.setProperty("snapshot.mode", "never"); //禁用快照读取，初始化使用DataEX
//        properties.setProperty("publication.autocreate.mode", "disabled"); // 禁用自动创建表的发布

        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("node5")
                .port(5432)
                .database("test") // monitor postgres database
                .schemaList("public,nsp")  // monitor inventory schema
                .tableList("public.t1,public.students,nsp.t2") // monitor products table
                .username("flink_cdc")
                .password("flink_cdc")
                .decodingPluginName("pgoutput")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .debeziumProperties(properties)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
