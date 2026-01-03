package com.zyy.study.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamUnboundeDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据，socket
        DataStreamSource<String> worldAndOne = env.socketTextStream("", 7777);
        // 3. 数据处理 ：切分、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneStream = worldAndOne.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (val, collector) -> {
            // 3.1 按照 空格 切分单词
            String[] words = val.split(" ");
            for (String word : words) {
                // 3.2 转换为 (word, 1)
                Tuple2<String, Long> wordAndOne = Tuple2.of(word, 1L);
                collector.collect(wordAndOne);
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));  // lambda 表达式会擦除类型
        KeyedStream<Tuple2<String, Long>, String> group = wordAndOneStream.keyBy(e -> e.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> result = group.sum(1);
        // 4. 输出数据
        result.print();
        // 5. 启动任务
        env.execute();
    }

}
