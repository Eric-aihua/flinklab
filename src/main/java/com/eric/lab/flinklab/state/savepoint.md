# 样例
以MaxValueKeyedStateExample程序为例，该程序会给收到的字母分配一个随机值，然后该程序通过state来记录
每个字母随机分配数字的最大值

1. 运行该程序：flink run  -c com.eric.lab.flinklab.state.MaxValueKeyedStateExample target/flinklab-1.0-SNAPSHOT.jar
2. 运行一段时间：从日志可以看出每个字母对应的随机值基本都接近10000
3. 生成checkpoint并停止应用：flink cancel -s savepoints 4470dc642db5d75e41c038e2a18e5601,执行该命令会在当前目录生成
一个savepoint文件夹
4. 用checkpoint文件夹从新启动应用：flink run -s /Users/eric/Tools/flink-1.10.1/bin/savepoints/savepoint-4470dc-648565d7d273 -c com.eric.lab.flinklab.state.MaxValueKeyedStateExample target/flinklab-1.0-SNAPSHOT.jar
该命令启动的应用会从cancel时的savepoint的状态中继续恢复（状态包括日志，每个key的最大值，checkpoint信息等内容）继续运行该程序。

# 注意事项
为了在单机环境下得到好的验证效果，在恢复的时候可以先将flink集群重启，然后再执行恢复程序