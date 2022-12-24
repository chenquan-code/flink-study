package com.cq.flink.streaming.basic.demo02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author chenquan
 * @Description 从socket实时读取数据统计word count
 * 使用ParameterTool从args获取参数，启动时必须设置环境变量 --hostname localhost --port 8888
 * 将结果数据封装到一个对象
 * @Date 2022-04-16 1:01
 **/

public class WordCount {

    public static void main(String[] args) throws Exception{
        //TODO 初始化程序入口 带Web UI 浏览器访问：http://localhost:8081/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //TODO 从args获取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStreamSource<String> streamSource = env.socketTextStream(parameterTool.get("hostname"),parameterTool.getInt("port"));
        SingleOutputStreamOperator<WordAndOne> wordCount = streamSource.flatMap(new SplitTask())
                .keyBy(WordAndOne::getWord)
                .sum("one");
        wordCount.print();
        env.execute();

    }


    static class SplitTask implements FlatMapFunction<String,WordAndOne>{
        @Override
        public void flatMap(String line, Collector<WordAndOne> collector) throws Exception {
            String[] split = line.split(",");
            for (String word : split){
                collector.collect(new WordAndOne(word,1));
            }
        }
    }

}
