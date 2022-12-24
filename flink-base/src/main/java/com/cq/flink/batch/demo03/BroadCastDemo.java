package com.cq.flink.batch.demo03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author chenquan
 * @Description 测试广播变量
 * @Date 2022-04-11 1:03
 **/

public class BroadCastDemo {

    public static void main(String[] args) throws Exception {
        //TODO 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> broadCastData = new ArrayList<>();
        broadCastData.add(new Tuple2<>(1,"广播数据1"));
        broadCastData.add(new Tuple2<>(2,"广播数据2"));
        broadCastData.add(new Tuple2<>(3,"广播数据3"));
        DataSet<Tuple2<Integer, String>> broadCastDataSource = env.fromCollection(broadCastData);
        DataSet<HashMap<Integer, String>> toBroadcast = broadCastDataSource.map(new MapFunction<Tuple2<Integer, String>, HashMap<Integer, String>>() {
            @Override
            public HashMap<Integer, String> map(Tuple2<Integer, String> input) throws Exception {
                HashMap<Integer, String> res = new HashMap<>();
                res.put(input.f0,input.f1);
                return res;
            }
        });

        //TODO 数据源
        DataSource<Integer> dataSource = env.fromElements(1, 2, 3);
        //TODO 获取广播变量值
        dataSource.map(new RichMapFunction<Integer, String>() {
            List<HashMap<Integer, String>> broadCastMap = new ArrayList<HashMap<Integer, String>>();
            Map<Integer, String> allMap = new HashMap<Integer, String>();
            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             * 所以，就可以在open方法中获取广播变量数据
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //3:获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(Integer key) throws Exception {
                String age = allMap.get(key);
                return key + "," + age;
            }
        }).withBroadcastSet(toBroadcast, "broadCastMapName")//2：执行广播数据的操作
                .print();

    }

}
