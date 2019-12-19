#!/bin/bash

mvn clean
mvn package -DskipTests
scp bin/*.* root@192.168.80.134:~/sc/flink_cb
scp target/flinklab-1.0-SNAPSHOT.jar root@192.168.80.134:~/sc/flink_cb