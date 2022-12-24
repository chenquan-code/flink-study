package com.cq.flink.batch.demo03;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @Author chenquan
 * @Description
 * @Date 2022-04-11 0:28
 **/

public class HashRangePartitionDemo {
    public static void main(String[] args) throws Exception {
        //TODO 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //TODO 添加数据源
        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        int i = 0;
        while (i < 100){
            int randomNumber = (int) (Math.random()*10);
            data.add(new Tuple2<>(randomNumber,"randomNumber:"+randomNumber));
            i++;
        }

        DataSource<Tuple2<Integer, String>> dataSource = env.fromCollection(data);
        dataSource.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
                Iterator<Tuple2<Integer, String>> iterator = iterable.iterator();
                while (iterator.hasNext()){
                    Tuple2<Integer, String> next = iterator.next();
                    System.out.println(Thread.currentThread().getId() + "->" + next);
                }
            }
        }).print();




    }

}
