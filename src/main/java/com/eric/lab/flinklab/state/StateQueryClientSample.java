package com.eric.lab.flinklab.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * 查询Flink程序，有状态标记的值.
 * @author Eric
 * @version 1.0
 * @date 2020/9/20 3:27 下午
 */
public class StateQueryClientSample {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        QueryableStateClient stateClient= null;
        try {
            stateClient = new QueryableStateClient("localhost",9069);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        String jobid="9160c26985bcdf1a52faa19bd8982b4c";
        String queryKey="d"; // 查询a这个字母对应的状态值
        ValueStateDescriptor<Integer> descriptor=new ValueStateDescriptor<Integer>
                ("least", TypeInformation.of(new TypeHint<Integer>() {}),0);
        // 查询d这个key此时在flink中对应的valuestate的值。
        CompletableFuture<ValueState<Integer>> resultFuture =stateClient.getKvState(JobID.fromHexString(jobid),"latestQueryableId",queryKey, BasicTypeInfo.STRING_TYPE_INFO,descriptor);
        System.out.println(resultFuture.get().value()); //? 如果不加这句话，下面thenAccept里的代码就不能执行，原因还需要再研究下？
        resultFuture.thenAccept(response ->{
            try {
                System.out.println("$$$$$$$$$$$$$");
                Integer res = response.value();
                System.out.println(">>>>>>>>>>>value state:"+res);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
