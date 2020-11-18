package com.eric.lab.flinklab.datastream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author yd
 * @version 1.0
 * @date 2020/6/17 11:09 下午
 */
public class KafkaSourceSample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        // 2. 定义读取方式，此处以文件为例，还可以是数据库，socket,消息中间件等
        Properties propsProducer = new Properties();
        propsProducer.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092");
        FlinkKafkaConsumer<String> consumer=new FlinkKafkaConsumer<String>("test",new SimpleStringSchema(),propsProducer);
        DataStreamSource<String> ds= env.addSource(consumer);
        ds.print();
        env.execute("execute read source operation");
    }
}
