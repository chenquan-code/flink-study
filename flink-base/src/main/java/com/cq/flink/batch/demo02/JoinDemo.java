package com.cq.flink.batch.demo02;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * @Author chenquan
 * @Description  实现Join操作，如果要实现Left Join 和 Right Join 在 with方法里面 使用 if (leftSource == null) 判断实现
 * @Date 2022-04-10 20:48
 **/

public class JoinDemo {

    public static void main(String[] args) throws Exception {

        //TODO 生成两个数据源
        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"ww"));

        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"18"));
        data2.add(new Tuple2<>(2,"22"));
        data2.add(new Tuple2<>(4,"14"));

        //TODO 获取离线程序入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 获取数据源
        DataSource<Tuple2<Integer, String>> dataSource1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> dataSource2 = env.fromCollection(data2);
        //TODO 执行Join操作
        JoinOperator.EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> result = dataSource1.join(dataSource2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> d1, Tuple2<Integer, String> d2) throws Exception {
                        //TODO 返回dataSource1的两个属性和datasource2的第二个属性
                        return new Tuple3<>(d1.f0, d1.f1, d2.f1);
                    }
                });
        result.print();


    }
}
