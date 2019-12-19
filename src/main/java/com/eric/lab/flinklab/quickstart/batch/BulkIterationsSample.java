package com.eric.lab.flinklab.quickstart.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class BulkIterationsSample {
    static int MAX_INTER = 100;
    public static void main(String args[]) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// Create initial IterativeDataSet
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(MAX_INTER);

        DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                double x = Math.random();
                double y = Math.random();
                System.out.println("x:"+x+" y:"+y+" i:"+i);
                return i + ((x * x + y * y < 1) ? 1 : 0);
            }
        });

// Iteratively transform the IterativeDataSet
        DataSet<Integer> count = initial.closeWith(iteration);

        count.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer count) throws Exception {
                System.out.println("count:"+count);
                return count / (double) MAX_INTER * 4;
            }
        }).print();

//        env.execute("Iterative Pi Example");
    }
}
