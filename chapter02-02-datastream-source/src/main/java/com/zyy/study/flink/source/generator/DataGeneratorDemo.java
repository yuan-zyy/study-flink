package com.zyy.study.flink.source.generator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * 数据生成器:
         * 第一个参数:
         */
        DataGeneratorSource<String> source = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "number: " + value;
            }
        }, 10, RateLimiterStrategy.perSecond(1), Types.STRING);

        DataStreamSource<String> datagenerator = env.fromSource(source, WatermarkStrategy.noWatermarks(), "datagenerator");
        datagenerator.print();

        env.execute();

    }

}
