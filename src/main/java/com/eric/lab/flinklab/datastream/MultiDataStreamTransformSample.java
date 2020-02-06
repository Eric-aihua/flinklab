package com.eric.lab.flinklab.datastream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

public class MultiDataStreamTransformSample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 定义数据
        DataStreamSource ds1=env.fromElements(new Tuple2("a","1"),new Tuple2("b","2"),new Tuple2("a","2"),new Tuple2("b","1"));
        DataStreamSource ds2=env.fromElements(new Tuple2("a","1"),new Tuple2("e","3"));
        DataStreamSource ds3=env.fromElements(1,2,3,4,5,7);
        // 3. union demo,合并相同的类型
//        unionDemo(ds1,ds2);
        // 4. connect可以连接不同类型的数据，返回值为统一类型
//        connectDemo(ds1,ds3);
        // 5. 通过split将数据进行拆分,通过结合select函数,对数进行输出
        splitDemo(ds3);

        env.execute("Transform demo!");
    }

    private static void splitDemo(DataStreamSource ds3) {
        SplitStream ss=ds3.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                String result = value %2==0 ? "even":"odd";
                return Arrays.asList(result);
            }
        });
        // 输出带有偶数标签的数据
        ss.select("even").print();
//        ss.select("odd").print();
    }

    private static void connectDemo(DataStreamSource ds1, DataStreamSource ds3) {
        //connect的结果需要经过map或是flatmap处理后，才能print()
        DataStream result = ds1.connect(ds3).map(new CoMapFunction<Tuple2<String,String>,Integer,Tuple2<String,String>>() {
            @Override
            //定义第一个ds的处理逻辑
            public Tuple2<String,String> map1(Tuple2<String,String> value) throws Exception {
                return value;
            }

            @Override
            //定义第二个ds的处理逻辑
            public Tuple2<String,String> map2(Integer value) throws Exception {
                return new Tuple2<String,String>("key"+value,value.toString());
            }
        });
        result.print();
    }


    private static void unionDemo(DataStreamSource ds1, DataStreamSource ds2) {

        DataStream result =ds1.union(ds2);
        result.print();
    }
}
