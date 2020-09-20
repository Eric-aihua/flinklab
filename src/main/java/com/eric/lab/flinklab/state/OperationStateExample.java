package com.eric.lab.flinklab.state;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Random;



/**
 * @author Eric
 * @version 1.0
 * @date 2020/7/28 10:53 下午
 */
public class OperationStateExample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启状态检查点
        env.enableCheckpointing(2000);
        //设置checkpoint模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);
        DataStream<String> ds = env.socketTextStream("localhost", 7777, "\n");
        ds.map(
                new RichMapFunction<String, Tuple2<String, Integer>>() {
                    // 使用累加器
                    private IntCounter numLines=new IntCounter();

                    @Override
                    public void open(Configuration  params) throws Exception{
                        super.open(params);
                        getRuntimeContext().addAccumulator("NumLines",this.numLines);
                    }

                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
//                        return Tuple2.of(value,1);
                        int intValue = new Random().nextInt(100);
                        this.numLines.add(1);
                        return Tuple2.of(value,intValue);
                    }
                }
        ).uid("MapOperations111").keyBy(0).flatMap(new CheckPointCountFunc()).uid("flatMapOperatios111").printToErr();
        env.execute("MaxValue");
    }

    /**
     * 该func主要通过keystate实现对每个key对应value数量的统计，以及通过使用operationState统计算子数据量的统计
     * @author Eric
     * @version 1.0
     * @date 2020/7/28 11:00 下午
     */
    public static class CheckPointCountFunc extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer,Integer>> implements CheckpointedFunction {
        private transient ValueState<Integer> keyedState;
        private transient ListState<Integer> operatorState;
        private int operatorCount=0;
        @Override
        //触发checkout时会被调用
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("#####checkout point was executed");
            // 发生snapshot时，重新计算operatorstate的count
            operatorState.clear();
            operatorState.add(operatorCount);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //第一次初始化的逻辑
            keyedState=context.getKeyedStateStore().getState(new ValueStateDescriptor<Integer>
                    ("keyedState", TypeInformation.of(new TypeHint<Integer>() {}),0));
            operatorState=context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>
                    ("keyedState", TypeInformation.of(new TypeHint<Integer>() {})));
            if(context.isRestored()){
                //从状态中进行恢复的逻辑
                operatorCount= IteratorUtils.toList((Iterator<Integer>) operatorState.get()).stream().mapToInt(x-> Integer.valueOf(x.toString())).sum();
            }
        }

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            int keyedCount=keyedState.value()+1;
            keyedState.update(keyedCount);
            operatorCount=operatorCount+1;
            out.collect(Tuple3.of(value.f0,keyedCount,operatorCount));
        }
    }
}
