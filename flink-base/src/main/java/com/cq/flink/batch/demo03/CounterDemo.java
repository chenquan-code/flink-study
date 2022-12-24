package com.cq.flink.batch.demo03;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * @Author chenquan
 * @Description 实现累加器
 * @Date 2022-04-11 12:31
 **/

public class CounterDemo {

    public static String outputPath = "D:\\WorkSpace\\StudyWorkSpace\\com.cq.flink-study\\src\\main\\data\\output\\countLine";

    public static void main(String[] args) throws Exception {
        //TODO 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 添加数据源
        DataSource<String> dataSource = env.fromElements("a", "b", "c", "d","e","f","g");
        //TODO 创建累加器
        DataSet<String> countLine = dataSource.map(new RichMapFunction<String, String>() {
            // 累加器 统计一共多少行
            private IntCounter countLine = new IntCounter();
            // 重写open方法，这个方法只会被执行一次
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 注册累加器
                getRuntimeContext().addAccumulator("countLine", countLine);
            }
            // 重写map，这个方法每次调用都会执行一次
            @Override
            public String map(String s) throws Exception {
                this.countLine.add(1);
                return s;
            }
        }).setParallelism(2);
        //TODO 写入文件
        countLine.writeAsText(outputPath);
        //TODO 获取累加器的值必须让作业运行
        JobExecutionResult counterDemo = env.execute("CounterDemo");
        //TODO 获取累加器的值
        int res = counterDemo.getAccumulatorResult("countLine");
        System.out.println(res);

    }

}
