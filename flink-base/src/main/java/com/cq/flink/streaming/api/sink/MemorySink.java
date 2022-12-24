package com.cq.flink.streaming.api.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author chenquan
 * @Description 将状态数据保存在内存中，然后读取内存的数据进行恢复
 * @Date 2022-04-17 19:56
 **/

public class MemorySink implements SinkFunction<Tuple2<String,Integer>>, CheckpointedFunction {

    //TODO 在内存中缓存的结果数据
    private List<Tuple2<String,Integer>> bufferList;
    //TODO 内存存储大小阈值
    private int threshold;
    //TODO 在状态中缓存的结果数据
    private ListState<Tuple2<String,Integer>> checkpointState;

    public MemorySink(){
        this.threshold = 2;
        this.bufferList = new ArrayList<>();
    }
    public MemorySink(int threshold){
        this.threshold = threshold;
        this.bufferList = new ArrayList<>();
    }

    /**
     *如果实现sink function 里面需要实现的是这个方法。
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        //TODO 可以将接收到的每一条数据保存到任何的存储系统中
        bufferList.add(value);
        //TODO 内存的数据达到一定数量之后输出，并清空缓存，例如写MySQL
        if (bufferList.size() >= threshold){
            System.out.println("这批数据攒够" + threshold + "条啦！" + bufferList);
            bufferList.clear();
        }
    }


    /**
     * 用于将内存中数据保存到状态中,会定期执行
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointState.clear();
        for (Tuple2<String,Integer> value : bufferList){
            checkpointState.add(value);
        }
//        checkpointState.addAll(bufferList);
    }

    /**
     * 用于在程序重启的时候从状态中恢复数据到内存
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<Tuple2<String, Integer>> bufferValue =
                new ListStateDescriptor<>("buffer value", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        checkpointState = context.getOperatorStateStore().getListState(bufferValue);
        //TODO 将State数据保存到内存中
        if (context.isRestored()) {
            for (Tuple2<String,Integer> value : checkpointState.get() ){
                bufferList.add(value);
            }
        }
    }
}
