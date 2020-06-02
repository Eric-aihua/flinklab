package com.eric.lab.flinklab.datastream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Random;

/**
 * 窗口函数演示事例
 */
public class WindowFuncSample {

    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> ds = env.socketTextStream("localhost", 7777, "\n");
        WindowedStream ws=ds.map(
                new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
//                        return Tuple2.of(value,1);
                        return Tuple2.of(value, new Random().nextInt(100));
                    }
                }
        ).keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)));
        // 2. 使用Reduce的方式对流数据进行处理
//        DataStream processResult = processdByReduce(ws);
        // 3. 使用aggregate的方式对窗口数据进行处理
//        DataStream processResult = processdByAggs(ws);
        // 4. 使用ProcessWindowFunction对一个窗口中的所有元素进行处理
        DataStream processResult = processdByWindow(ws);
        // 5. 可以使用Reduce+ProcessWindowFunction的方式实现更为复杂的窗口操作。此处不再掩饰
        processResult.print();
        env.execute("func test");
    }
// 利用ProcessWindowFunction函数，实现对一个窗口数据的统计。
    private static DataStream processdByWindow(WindowedStream ws) {
        // 1. Key的类型该位置变成了Tuple1<String>,而不是String
        return ws.process(new ProcessWindowFunction<Tuple2<String,Integer>, String,Tuple1<String>,TimeWindow>() {
            @Override
            public void process(Tuple1<String> s, Context context, Iterable<Tuple2<String,Integer>> elements, Collector<String> out) throws Exception {
                ArrayList<Tuple2<String,Integer>> elementList=Lists.newArrayList(elements);
                long count =elementList.stream().count();
                long eTime =context.window().getEnd();
                long sTime =context.window().getStart();
                // 2. ProcessWindowFunction提供了对state操作的能力，次数不再演示
//                context.globalState().getMapState().....
                int min = (int) elementList.stream().map(x->Integer.valueOf(x.f1)).sorted().findFirst().get();
                StringBuilder sb =new StringBuilder();
                sb.append("stime:").append(sTime).append(" etime:").append(eTime).
                        append("key:").append(s.f0).append(" count:").append(count).
                        append(" min:").append(min);
                out.collect(sb.toString());
            }
        });
    }

    // 通过agg方式实现平均数的求值
    private static DataStream processdByAggs(WindowedStream ws) {
        DataStream ds = ws.aggregate(new AggregateFunction<Tuple2<String,Integer>,Tuple2<Integer,Integer>,Double>() {

            @Override
            public Tuple2<Integer, Integer> createAccumulator() {
                return Tuple2.of(0,0);
            }

            @Override
            public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> accumulator) {
                return Tuple2.of(accumulator.f0+value.f1,accumulator.f1+1);
            }

            @Override
            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                return Double.valueOf(accumulator.f0/accumulator.f1);
            }

            @Override
            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
            }
        });
        return ds;
    }

    protected static DataStream processdByReduce(WindowedStream ws) {
        DataStream ds= ws.reduce(new ReduceFunction<Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> reduce(Tuple2<String,Integer> value1, Tuple2<String,Integer> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1+value2.f1);
            }
        });
        return ds;
    }

}
