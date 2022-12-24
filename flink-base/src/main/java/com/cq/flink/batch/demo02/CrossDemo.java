package com.cq.flink.batch.demo02;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;
import java.util.List;

/**
 * @Author chenquan
 * @Description 实现笛卡尔积
 * @Date 2022-04-10 21:59
 **/

public class CrossDemo {
    public static void main(String[] args) throws Exception {
        //TODO 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 学生姓名
        List<String> students = Arrays.asList("张三", "李四");
        //TODO 考试科目
        List<String> courses = Arrays.asList("高数", "英语");
        //TODO 这两个科目都是必修课，每个学生都需要学，因此使用笛卡尔积
        DataSource<String> studentsSource = env.fromCollection(students);
        DataSource<String> coursesSource = env.fromCollection(courses);
        CrossOperator.DefaultCross<String, String> cross = studentsSource.cross(coursesSource);
        //TODO 输出控制台
        cross.print();
    }
}
