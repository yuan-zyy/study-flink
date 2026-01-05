package com.zyy.study.flink.source.file;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从文件读取数据: 新 source 架构
        String filePath = "H:\\ProjectSource\\Idea\\study-flink\\chapter01-word-count\\input\\word.txt";
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(filePath)).build();
        DataStreamSource<String> objectDataStreamSource = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource");

        objectDataStreamSource.print();
        env.execute();
    }
    
}
