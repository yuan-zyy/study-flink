package com.zyy.study.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Data Set API 实现 word count (不推荐)
 */
public class WordCountBatchDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据，从文件中
        DataSource<String> lineDS = env.readTextFile("chapter01-word-count/input/word.txt");

        // 3. 切分、转换 (word, 1)
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String lineVal, Collector<Tuple2<String, Long>> collector) throws Exception {
                // 3.1 按照 空格 切分单词
                String[] words = lineVal.split(" ");
                // 3.2 将 单词 转化为 (word, 1)
                for (String word : words) {
                    Tuple2<String, Long> wordTuple2 = Tuple2.of(word, 1L);
                    // 3.3 适用 Collector 向下游发送数据
                    collector.collect(wordTuple2);
                }
            }
        });

        // 4. 按照 word 分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOne.groupBy(0);

        // 5. 各分组内
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1); // 1 是位置，表示的是第二个字段

        // 6. 打印输出
        sum.print();
    }

}
