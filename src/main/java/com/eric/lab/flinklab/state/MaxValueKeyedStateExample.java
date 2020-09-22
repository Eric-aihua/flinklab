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
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 1.  状态管理
 * 2. Rocksdb作为状态管理器
 * 3. 设置Queryable ,使状态可查询,需要执行两个操作：（1）将flink 安装目录中，opt目录下的flink-queryable-state-runtime_2.12-1.10.1.jar
 * 拷贝到 lib目录下 （2）在应用程序代码中，调用StateDescriptor的setQueryble方法  (3)set the property queryable-state.enable to tru （4）使用客户端进行调用
 * @author Eric
 * @version 1.0
 * @date 2020/7/14 10:57 下午
 */
public class MaxValueKeyedStateExample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new RocksDBStateBackend("file:///Users/eric/Sourcecode/github/flinklab/checkpoints"));
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
        descriptor.setQueryable("latestQueryableId");
        max=getRuntimeContext().getState(descriptor);

    }

}

