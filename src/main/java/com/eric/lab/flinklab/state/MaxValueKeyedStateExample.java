package com.eric.lab.flinklab.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @author Eric
 * @version 1.0
 * @date 2020/7/14 10:57 下午
 */
public class MaxValueKeyedStateExample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> ds = env.socketTextStream("localhost", 7777, "\n");
        ds.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
//                        return Tuple2.of(value,1);
                        int intValue = new Random().nextInt(10000);
                        return Tuple2.of(value,intValue);
                    }
                }
        ).keyBy(0).flatMap(new MaxValueFunc()).print();
    env.execute("MaxValue");
    }

}

// 通过状态计算，求出每个Key的最大值
class MaxValueFunc extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer,Integer>> {

    private transient ValueState<Integer> max;

    @Override
    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple3<String, Integer,Integer>> out) throws Exception {
        int currentLeast=0;
        if (max.value()!=null){
            currentLeast=max.value();
        }
        if(value.f1>currentLeast){
            max.update(value.f1);
            out.collect(Tuple3.of(value.f0,value.f1,value.f1));
        }else{
            out.collect(Tuple3.of(value.f0,value.f1,currentLeast));
        }

    }

    @Override
    public void open(Configuration config){
        StateTtlConfig ttlConfig=StateTtlConfig.newBuilder(Time.seconds(10))
                //只在创建和写入时更新ttl
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                //对于过期的数据不可见
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();
        ValueStateDescriptor<Integer> descriptor=new ValueStateDescriptor<Integer>
                ("least", TypeInformation.of(new TypeHint<Integer>() {}),0);
//        descriptor.enableTimeToLive(ttlConfig);
        max=getRuntimeContext().getState(descriptor);

    }

}

