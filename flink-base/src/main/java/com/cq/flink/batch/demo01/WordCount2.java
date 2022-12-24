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
 * @Description 词频统计，将MapPartition封装成自己的类
 * @Date 2022-04-10 19:41
 **/

public class WordCount2 {

    public static String inputPath = "D:\\WorkSpace\\StudyWorkSpace\\com.cq.flink-study\\src\\main\\input\\hello.txt";
    public static String outputPath = "D:\\WorkSpace\\StudyWorkSpace\\com.cq.flink-study\\src\\main\\output\\wordCount.txt";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile(inputPath);
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = dataSource.flatMap(new MySplitTask());
        AggregateOperator<Tuple2<String, Integer>> wordCount = wordAndOne.groupBy(0).sum(1);
        wordCount.print();
    }

}

/**
 * 自定义字符串切割类
 */
class MySplitTask implements FlatMapFunction<String, Tuple2<String,Integer>>{

    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = line.split(",");
        for (String word : words){
            collector.collect(new Tuple2<>(word,1));
        }
    }
}



