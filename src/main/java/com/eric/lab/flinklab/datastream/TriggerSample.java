package com.eric.lab.flinklab.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import java.util.Random;

/**
 * Trigger使用以及自定义Trigger 例子
 */
public class TriggerSample extends WindowFuncSample {

    public static void main(String[] args) throws Exception {


        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> ds = env.socketTextStream("localhost", 7777, "\n");
        WindowedStream ws = ds.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
//                        return Tuple2.of(value,1);
                        int intValue = new Random().nextInt(10);
                        System.out.println("key:"+value+" value:"+intValue);
                        return Tuple2.of(value,intValue);
                    }
                }
        ).keyBy(0)
                .countWindow(3);
        DataStream processResult = processdByReduce(ws);
        processResult.print();
        env.execute("func test");
    }
}