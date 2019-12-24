package com.eric.lab.flinklab.model;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 主要用来演示java lambda 在flink的使用规则
 */

public class JavaLambdaSample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 1. map 中lambda输出的类型可以自动判断，不需要指定
        DataSet<Integer> ds = env.fromElements(1, 2, 3, 4);
        ds.map(x -> x * x).print();
        // 2. flatMap中的lambda由于编译器类型擦除的原因，会导致申明的类型丢失，需要使用returns方法显示的指定返回类型，
        // 否则会报“The generic type parameters of 'Collector' are missing.”的错误
        ds.flatMap((Integer number, Collector<String> out) -> {
            out.collect(number + "" + number);
        }).returns(Types.STRING).print();
        // 3. 对于map中使用tuple会有同样的类型问题，解决方案可参考官网：https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/java_lambdas.html

    }
}
