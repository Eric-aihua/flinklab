package com.eric.lab.flinklab.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 演示对延迟数据的处理
 * 样例演示数据
 */
public class LatenessDataSample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 设置EventTime作为流处理的时间
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 便于测试，将并行度设置为1
        env.setParallelism(1);
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};
        DataStream<String> ds = env.socketTextStream("localhost", 7777, "\n");
        // 3. 简单的map操作
        DataStream<Tuple2<String, Long>> inputInfo = ds.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
//                System.out.println("==="+value);
                String fields[] = value.split(",");
                Tuple2<String,Long> result=null;
                try{
                    result=new Tuple2<String, Long>(fields[0], Long.valueOf(fields[1]));
                }catch(Exception ex){
                    result = new Tuple2<String, Long>(fields[0], Long.valueOf(-1));
                }
                return result;

            }
        });
        SingleOutputStreamOperator assignTsAndW= inputInfo.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element) {
//                System.out.println(element);
                return element.f1;
            }
        });
        // 1. 通过allowedLateness指定最大允许的延迟时间 （2）通过sideOutputLateData 设置超过最大延迟时间数据的flag
        SingleOutputStreamOperator result =assignTsAndW.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(1))).
                allowedLateness(Time.seconds(3)).sideOutputLateData(outputTag).reduce(new ReduceFunction<Tuple2<String,Long>>() {
            @Override
            public Tuple2<String,Long> reduce(Tuple2<String,Long> value1, Tuple2<String,Long> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1+value2.f1);
            }
        });


        // 获取超过最大延迟的数据
        result.getSideOutput(outputTag).print();
        result.print();
        env.execute("Lateness data process sample！");
    }
}
