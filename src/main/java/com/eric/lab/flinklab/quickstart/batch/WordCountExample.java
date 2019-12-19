package com.eric.lab.flinklab.quickstart.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 *
 */
public class WordCountExample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //使用本地环境
//        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter())
                .groupBy(0) // 使用tuple的第一个元素作为key
// 可以通过reduce函数或是内嵌的sum函数实现统计
//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
//                        return new Tuple2<>(stringIntegerTuple2.f0,stringIntegerTuple2.f1+t1.f1);
//                    }
//                });
                .sum(1); // 相同的key的tuple,使用第二个元素作为value
        wordCounts.mapPartition(new MapPartitionFunction<Tuple2<String,Integer>, Object>() {
            @Override
            public void mapPartition(Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {
                for(Tuple2<String,Integer> tuple:iterable){
                    System.out.println("mapPartition:"+tuple.toString());
                }
            }
        });
        // 对结果进行排序
        wordCounts.sortPartition(0, Order.ASCENDING).print();

        // 将结果自定义格式并写到结果文件中
        wordCounts.writeAsFormattedText("/tmp/flink/output/word_count", FileSystem.WriteMode.OVERWRITE,new TextOutputFormat.TextFormatter<Tuple2<String, Integer>>() {
            @Override
            public String format(Tuple2<String, Integer> value) {
//                System.out.println(value);
                return value.f1+"-"+value.f0;
            }
        });
        env.execute("batch wordcount");

    }
// 字符串分割实现
    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
