package com.cq.flink.streaming.state.operatorstate;

import com.cq.flink.streaming.api.sink.MemorySink;
import com.cq.flink.util.FlinkUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author chenquan
 * @Description 将状态数据保存在内存中，然后读取内存的数据进行恢复
 *
 * @Date 2022-04-17 19:47
 **/

public class OperatorState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtil.getStreamExecutionEnvironment(true);

        DataStreamSource<Tuple2<String, Integer>> dataStreamSource =
                env.fromElements(
                        Tuple2.of("Spark", 3),
                        Tuple2.of("Flink", 5),
                        Tuple2.of("Hadoop", 7),
                        Tuple2.of("Spark", 4));


        //TODO 必须设置并行度为1，数据分散到不然多个task，判断条件>2就比较难满足，数据量大的话无所谓
        dataStreamSource.addSink(new MemorySink(2)).setParallelism(1);
//        dataStreamSource.print();

        env.execute("OperatorState");

    }
}
