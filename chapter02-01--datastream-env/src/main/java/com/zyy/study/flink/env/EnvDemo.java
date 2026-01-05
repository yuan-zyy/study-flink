package com.zyy.study.flink.env;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(); // 自动
//        env.fromSource()

    }

}
