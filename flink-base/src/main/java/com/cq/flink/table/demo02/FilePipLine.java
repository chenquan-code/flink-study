package com.cq.flink.table.demo02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @Author chenquan
 * @Description 实现从文件中读取数据变成table
 * @Date 2022-05-01 14:58
 **/

public class FilePipLine {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 文件路径
        String input = "D:\\WorkSpace\\StudyWorkSpace\\com.cq.flink-study\\src\\main\\data\\input\\hello.txt";
        //TODO 读取文件变成一个table
        tableEnv.connect(new FileSystem().path(input))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("c1", DataTypes.STRING())
                        .field("c2",DataTypes.STRING()))
                .createTemporaryTable("t1");

        //TODO Table API
        Table t1 = tableEnv.from("t1");
        Table tableResult = t1.select("c1,c2").filter("c1 === 'hello'");

        //TODO SQL API
        Table sqlResult = tableEnv.sqlQuery("select c1,c2 from t1 where c1 = 'hello'");

        //TODO 输出到文件
        String output = "D:\\WorkSpace\\StudyWorkSpace\\com.cq.flink-study\\src\\main\\data\\output\\output.txt";
        tableEnv.connect(new FileSystem().path(output))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("c1",  DataTypes.STRING())
                        .field("c2",DataTypes.STRING()))
                .createTemporaryTable("t2");

        tableResult.insertInto("t2");

        env.execute();


    }


}
