package com.zyy.study.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.KeySelector;

public class WordCountStreamDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据
        // 从文件读取
        DataStreamSource<String> lineStreamDS = env.readTextFile("chapter01-word-count/input/word.txt");
//        DataStreamSource<String> lineStreamDS = env.socketTextStream("106.15.90.55", 7777);

        // 3. 处理数据: 切分 、转换 、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneStream = lineStreamDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String lineVal, Collector<Tuple2<String, Long>> collector) throws Exception {
                        // 3.1 按照 空格 切分单词
                        String[] words = lineVal.split(" ");
                        // 3.2 将 单词 转化为 (word, 1)
                        for (String word : words) {
                            Tuple2<String, Long> wordTuple2 = Tuple2.of(word, 1L);
                            collector.collect(wordTuple2);
                        }
                    }
                });

        // 3.3 按照 word 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneStreamGroup = wordAndOneStream.keyBy(e -> e.f0);

        // 3.4 按照分组聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneStreamGroup.sum(1);

        // 4. 输出数据
        result.print();

        // 5. 执行: 类似 SparkStreaming 最后的 start()
        env.execute();

    }

}
