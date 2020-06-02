package com.eric.lab.flinklab.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TableWordCountSample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("127.0.0.1", 7777, "\n");
    }
}
