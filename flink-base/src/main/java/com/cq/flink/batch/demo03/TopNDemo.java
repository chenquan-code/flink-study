package com.cq.flink.batch.demo03;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * @Author chenquan
 * @Description 实现各种排序效果
 * @Date 2022-04-10 23:20
 **/

public class TopNDemo {

    public static void main(String[] args) throws Exception {
        //TODO 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 添加数据源
        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2,"zs"));
        data.add(new Tuple2<>(4,"ls"));
        data.add(new Tuple2<>(3,"ww"));
        data.add(new Tuple2<>(1,"zl"));
        data.add(new Tuple2<>(1,"tq"));
        data.add(new Tuple2<>(1,"wb"));
        data.add(new Tuple2<>(2,"tj"));
        data.add(new Tuple2<>(4,"aa"));
        //TODO 加载数据源
        DataSource<Tuple2<Integer, String>> dataSource = env.fromCollection(data);
        //TODO 正常输出
        System.out.println("正常输出:"+data);
        //TODO 按插入顺序取Top N
        dataSource.first(3).print();
        //TODO 按照第一列的值分组，每组取Top 2个，类似于SQL里面的开窗函数
        dataSource.groupBy(0).first(2).print();
        //TODO 按照第一列的值分组，第二列排序，组取Top 2个
        dataSource.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
    }

}
