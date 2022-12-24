package com.cq.flink.table.demo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author chenquan
 * @Description
 * @Date 2022-04-29 1:43
 **/

public class WordCount {


    public static void main(String[] args) throws Exception {
        //TODO 步骤一：获取离线程序入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //TODO 步骤二：生成数据
        DataSource<String> dataSource = env.fromElements("hello com.cq.flink", "hello spark");

        BatchTableEnvironment tblEnv = BatchTableEnvironment.create(env);

        FlatMapOperator<String, WordAndOne> input = dataSource.flatMap(new FlatMapFunction<String, WordAndOne>() {

            @Override
            public void flatMap(String input, Collector<WordAndOne> collector) throws Exception {
                String[] words = input.split(" ");
                for (String word : words) {
                    collector.collect(new WordAndOne(word, 1));
                }
            }
        });

        //TODO 创建一张表
        Table table = tblEnv.fromDataSet(input);

        //TODO 单词计数
        Table data = table.groupBy("word").select("word,wordCount.sum as wordCount");

        DataSet<WordAndOne> result = tblEnv.toDataSet(data, WordAndOne.class);

        result.print();


    }



}
