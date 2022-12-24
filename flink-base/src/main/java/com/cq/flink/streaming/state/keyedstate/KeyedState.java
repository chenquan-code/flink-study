package com.cq.flink.streaming.state.keyedstate;

import com.cq.flink.streaming.api.transform.SplitWordAndValueTask;
import com.cq.flink.streaming.api.transform.WindowAverageTask;
import com.cq.flink.util.FlinkUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author chenquan
 * @Description 实现接收到同一个Key的元素达到三个后，触发计算这三个元素的平均值
 * nc -lp 8888
 * 输入：
    a,3
    a,2
    a,4
   输出：
    (a,3)

 * @Date 2022-04-17 15:42
 **/

public class KeyedState {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtil.getStreamExecutionEnvironment(true);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 8888);

        streamSource
                .flatMap(new SplitWordAndValueTask())
                .keyBy(tuple -> tuple.f0)
                .flatMap(new WindowAverageTask())
                .print();

        env.execute("KeyedState01");

    }
}
