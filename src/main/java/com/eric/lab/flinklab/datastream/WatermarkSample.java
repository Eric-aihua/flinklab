package com.eric.lab.flinklab.datastream;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 演示watermark的使用方法
 * 演示步骤
 *（1）在终端中启动nc -l 127.0.0.1 7777
 * (2)启动该程序
 *（3）在终端中依次输入
 * 000001,1461756862000
 * 000001,1461756866000
 * 000001,1461756872000
 * 000001,1461756873000
 * 000001,1461756874000  //此时触发第一次window操作。
 * 000001,1461756876000
 * 000001,1461756877000
 *
 * */

public class WatermarkSample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 设置EventTime作为流处理的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 便于测试，将并行度设置为1
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);
        DataStream<String> ds = env.socketTextStream("localhost", 7777, "\n");
        // 3. 简单的map操作
        DataStream<Tuple2<String, Long>> inputInfo = ds.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
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

        // 4. 自定义event以及watermark的生成函数，此处以Periodic类型的watermark为例
        DataStream waterMarkStream = inputInfo.assignTimestampsAndWatermarks(new PeriodicWatermarkSample());
        // 5. 执行窗口操作
//        DataStream windowStream = waterMarkStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(3))).apply(new WindowFunctionSample());
        DataStream windowStream = waterMarkStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(3))).apply(new WindowFunctionSample());
        windowStream.print();

        env.execute("watermark sample");
    }


}

// 自定义窗口处理函数
class WindowFunctionSample implements org.apache.flink.streaming.api.functions.windowing.WindowFunction<Tuple2<String,Long>,String, Tuple1<String>, TimeWindow> {

    @Override
    public void apply(Tuple1<String> key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
        List<Tuple2<String,Long>> list =Lists.newArrayList();
        for(Tuple2<String,Long> element:input){
            list.add(element);
        }
        list.sort((Comparator<Tuple2<String, Long>>) (o1, o2) -> o1.f1.compareTo(o2.f1));
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        StringBuilder output = new StringBuilder();
        output.append(key+"#");
        output.append(list.size()+"#");
        output.append(simpleDateFormat.format(list.get(0).f1)+"#");
        output.append(simpleDateFormat.format(list.get(list.size()-1).f1)+"#");
        output.append(simpleDateFormat.format(window.getStart())+"#");
        output.append(simpleDateFormat.format(window.getEnd())+"#");
        out.collect(output.toString());
    }
}

// 自定义PeriodicWatermark
class PeriodicWatermarkSample implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
    private long currentMaxTS = 0l;
    private long maxOutOfOrderness = 10000l;
    private Watermark watermark;
    private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        watermark = new Watermark(currentMaxTS - maxOutOfOrderness);
//        System.out.println("Obj:"+this+" currentMaxTS:"+currentMaxTS);
        return watermark;
    }

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
        long ts = element.f1;
        this.currentMaxTS = Math.max(ts, this.currentMaxTS);
        System.out.println("#element:"+element);
        System.out.println("#watermark:"+watermark);
        //System.out.println("timestamp:" + element.f0 + ",f1:" + element.f1 + "|format f1:" + format.format(element.f1) + ",currentMaxTS：" + currentMaxTS + "|format currentMaxTS:" + format.format(currentMaxTS) + ",watermark:" + watermark.toString());
        return ts;
    }
}
