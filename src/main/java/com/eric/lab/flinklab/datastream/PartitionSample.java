package com.eric.lab.flinklab.datastream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * 演示分区的机制
 * */
public class PartitionSample {
    public static void main(String[] args) throws Exception {
        // 1. 定义Env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义数据
        DataStreamSource ds1=env.fromElements(new Tuple2("a","1"),new Tuple2("b","2"),new Tuple2("a","2"),new Tuple2("b","1"));
        DataStreamSource ds2=env.fromElements(new Tuple2("a","1"),new Tuple2("e","3"));
        DataStream ds3= ds1.union(ds2);
        // 3. 默认分区
//        ds3.print("default");
        // 4. 随机分区,容易失去原有的分区结构
//        ds3.shuffle().print("shuffle");
//        // 5. Roundrobin方式，分区比较均衡，但数据会全局的通过网络传输到其他节点
//        ds3.rebalance().print("roundrobin");
//        // 6. rescaling方式，只对上下游继承关系的数据进行平衡，例如上游的并发度是2，下游的并发度是4，则每个上游会将数据分到均匀的分发到各自的下游
//        ds3.rescale().print("rescale");
          // 7. 自定义分区方式
        ds3.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                // 使用key首字母ascii%numPartitions算出对应的分区
                int asciiIndex=key.charAt(0);
                return asciiIndex%numPartitions;  
            }
        },0).print();

        env.execute("execute read source operation");

    }
}
