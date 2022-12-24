package com.cq.flink.streaming.state.operatorstate;

import com.cq.flink.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author chenquan
 * @Description 控制流控制输入流的过滤，实现动态改变过滤规则
 * @Date 2022-04-18 22:47
 **/

public class BroadcastState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtil.getStreamExecutionEnvironment(true);

        //TODO 数据流
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        //TODO 控制流
        DataStreamSource<String> controlStream = env.socketTextStream("localhost", 8888);

        //TODO 把控制流变成（key,word）
        SingleOutputStreamOperator<Tuple2<String, String>> control = controlStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String word) throws Exception {
                return Tuple2.of("key", word);
            }
        });
        //TODO 广播descriptor
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("ControlStream", String.class, String.class);
        //TODO 广播控制流
        BroadcastStream<Tuple2<String, String>> broadcast = control.broadcast(descriptor);
        //TODO 数据流连接广播流
        dataStream.connect(broadcast)
                .process(new KeyWordsCheckProcessor())
                .print();

        env.execute();

    }
}
