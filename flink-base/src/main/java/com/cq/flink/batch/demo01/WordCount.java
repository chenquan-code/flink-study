package com.cq.flink.batch.demo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author chenquan
 * @Description 测试 Flink WordCount 程序，最简单的做法
 * 注意，这里的Tuple2使用的是flink包下的而不是scala包
 * @Date 2022-04-10 18:42
 **/

public class WordCount {
    public static String inputPath = "D:\\WorkSpace\\StudyWorkSpace\\com.cq.flink-study\\src\\main\\data\\input\\hello.txt";
    public static String outputPath = "D:\\WorkSpace\\StudyWorkSpace\\com.cq.flink-study\\src\\main\\data\\output\\wordCount.txt";

    public static void main(String[] args) throws Exception {
        //TODO 步骤一：获取离线程序入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 步骤二：获取数据源 这里一本地文件为例
        DataSource<String> dataSource = env.readTextFile(inputPath);
        //TODO 步骤三：将数据打平并组合成(word , 1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(",");
                for (String word : words) {
                    // 将单词变成 (word , 1)
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        //TODO 步骤四：按第一个Key分组按第二个key求和
        AggregateOperator<Tuple2<String, Integer>> wordCount = wordAndOne.groupBy(0).sum(1);
        //TODO 步骤五：将结果写到文件
        wordCount.writeAsText(outputPath).setParallelism(1);
//        wordCount.print();
        //TODO 步骤六：启动程序
        env.execute("WordCountApp");



    }
}
