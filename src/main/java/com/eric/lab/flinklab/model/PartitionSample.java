package com.eric.lab.flinklab.model;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
/**
 * 演示按照不同的分区方法对数据集进行分区
 * */

public class PartitionSample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String,String>> firstDS=env.fromElements(new Tuple2("a","1"),new Tuple2("b","2"),new Tuple2("a","2"),new Tuple2("b","1"));
//        // 1. 按照位置指定分区的key的位置，下例中分别使用Tuple2的第一个以及第二个字段进行分区
        firstDS.groupBy(0).reduce(new AccuReduceFunction()).print();
        firstDS.groupBy(1).reduce(new AccuReduceFunction()).print();
        // 2. 按照字段名称进行,返回每个分组中，岁数大的student.
        System.out.println("---------------------2. group by field name");
        DataSet<Student> secondDS=env.fromElements(new Student("a",1),new Student("b",9),
                new Student("a",2),new Student("b",1));
        secondDS.groupBy("name").reduce(new MaxAgeReduceFunction()).print();
        // 3. 使用KeySelector实现分区
        System.out.println("---------------------3. group by key selector");
        secondDS.groupBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student student) throws Exception {
                return student.getName();
            }
        }).reduce(new MaxAgeReduceFunction()).print();

    }
}

// 按照位置进行分区使用
class AccuReduceFunction implements ReduceFunction<Tuple2<String,String>> {

    @Override
    public Tuple2<String,String> reduce(Tuple2<String,String> t1, Tuple2<String,String> t2) throws Exception {
        return new Tuple2<String,String>(t1.f0,t1.f1+"#"+t2.f1);
    }
}

class MaxAgeReduceFunction implements ReduceFunction<Student> {
    @Override
    public Student reduce(Student t0, Student t1) throws Exception {
        if(t0.getAge()>= t1.getAge()){
            return t0;
        }else{
            return t1;
        }
    }
}

