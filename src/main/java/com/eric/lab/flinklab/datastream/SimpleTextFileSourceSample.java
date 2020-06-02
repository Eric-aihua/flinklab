package com.eric.lab.flinklab.datastream;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class SimpleTextFileSourceSample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputPath="/Users/eric/Sourcecode/github/flinklab/src/main/java/com/eric/lab/flinklab/datastream/test.csv";

        //FileProcessingMode参数决定了数据数据的模式：
        // PROCESS_CONTINUOUSLY:每次文件发生变化，都会全部读取
        // PROCESS_ONCE:文件变化的时候，只会读取变化的部分
        // 2. 定义读取方式，此处以文件为例，还可以是数据库，socket,消息中间件等
        DataStreamSource<String> ds= env.readFile(new TextInputFormat(new Path(inputPath)) ,inputPath, FileProcessingMode.PROCESS_ONCE,2);
        // 3. 执行转化与输出
        ds.map(v -> v.toUpperCase()).writeAsText("./output", FileSystem.WriteMode.OVERWRITE);
        // 4. 执行操作
        env.execute("execute read source operation");

    }
}
