package com.eric.lab.flinklab.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 使用kafka_producer.py产生测试数据
 * @author Eric
 * @version 1.0
 * @date 2020/11/11 11:07 下午
 */
public class KafkaSourceWithJsonFormatSample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment=StreamTableEnvironment.create(env);
        String sql="CREATE TABLE users (\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  address STRING\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'test_flink_table',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'format.type' = 'json'\n" +
                ")";
        //从kafka收取消息，并注册一个名字为users的表
        tableEnvironment.executeSql(sql);
        //对table进行查询
        Table result=tableEnvironment.sqlQuery("select * from users where age > 20");
        //将table查询的结果转成stream
        DataStream<Row> dsRow = tableEnvironment.toAppendStream(result, Row.class);
        dsRow.print();
        env.execute("execute read source operation");
    }
}
