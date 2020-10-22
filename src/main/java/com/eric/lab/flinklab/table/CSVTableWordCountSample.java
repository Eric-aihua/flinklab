package com.eric.lab.flinklab.table;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class CSVTableWordCountSample {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);
        TableSource csvSource=CsvTableSource.builder().path("/Users/eric/Sourcecode/github/flinklab/src/main/java/com/eric/lab/flinklab/table/test.csv").ignoreFirstLine()
                .fieldDelimiter(",")
                .field("id", Types.INT())
                .field("name", Types.STRING())
                .field("address", Types.STRING())
                .build();
        fbTableEnv.registerTableSource("employee",csvSource);
        Table table = fbTableEnv.scan("employee").where("name='simon'").select("id,name,address");
        DataSet<Student> result=fbTableEnv.toDataSet(table,Student.class);
        table.printSchema();
        result.print();
//        fbEnv.execute("ssss");

    }


}
