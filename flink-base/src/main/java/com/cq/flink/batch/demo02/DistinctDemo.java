package com.cq.flink.batch.demo02;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;

import java.util.Arrays;
import java.util.List;

/**
 * @Author chenquan
 * @Description 实现 distinct 操作
 * @Date 2022-04-10 20:48
 **/

public class DistinctDemo {

    public static void main(String[] args) throws Exception {

        //TODO 生成两个数据源
        List<String> list = Arrays.asList("hello","spark","spark","spark","flink");
        //TODO 获取离线程序入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 获取数据源
        DataSource<String> dataSource = env.fromCollection(list);
        //TODO 执行distinct操作
        DistinctOperator<String> distinct = dataSource.distinct();
        //TODO 输出控制台
        distinct.print();

    }
}
