package com.cq.flink.util;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author chenquan
 * @Description 封装程序入口等工具类  使用单例模式-双重检查懒汉式
 * @Date 2022-04-17 1:18
 **/

public class FlinkUtil {

    private static volatile ExecutionEnvironment env;
    private static volatile StreamExecutionEnvironment streamEnv;

    private FlinkUtil() {
    }

    /**
     * 获取批处理编程入口程序
     *
     * @return
     */
    public static ExecutionEnvironment getExecutionEnvironment(Boolean webUI) {
        if (env == null) {
            synchronized (FlinkUtil.class) {
                if (env == null) {
                    if (!webUI) {
                        env = ExecutionEnvironment.getExecutionEnvironment();
                    } else {
                        env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
                    }
                }
            }
        }
        return env;
    }

    /**
     * 获取流式编程入口程序
     *
     * @return
     */
    public static StreamExecutionEnvironment getStreamExecutionEnvironment(Boolean webUI) {
        if (streamEnv == null) {
            synchronized (FlinkUtil.class) {
                if (streamEnv == null) {
                    if (!webUI) {
                        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
                    } else {
                        streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
                    }
                }
            }
        }
        return streamEnv;
    }
}
