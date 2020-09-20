package com.eric.lab.flinklab.table;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

public class TableWordCountSample {
    public static void main(String[] args) {

        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

    }
}
