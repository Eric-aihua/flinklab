package com.eric.lab.flinklab.datastream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.time.ZoneId;
import java.util.Properties;

public class FlinkParquetUtils {
    private final static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private final static Properties props = new Properties();

    static {
        /** Set flink env info. */
        env.enableCheckpointing(60 * 1000);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /** Set kafka broker info. */
        props.setProperty("bootstrap.servers", "dn1:9092,dn2:9092,dn3:9092");
        props.setProperty("group.id", "flink_group_parquet");
        props.setProperty("kafka.topic", "flink_parquet_topic_d");

        /** Set hdfs info. */
        props.setProperty("hdfs.path", "hdfs://cluster1/flink/parquet");
        props.setProperty("hdfs.path.date.format", "yyyy-MM-dd");
        props.setProperty("hdfs.path.date.zone", "Asia/Shanghai");
        props.setProperty("window.time.second", "60");

    }

    /** Consumer topic data && parse to hdfs. */
    public static void getTopicToHdfsByParquet(StreamExecutionEnvironment env, Properties props) {
        try {
            String topic = props.getProperty("kafka.topic");
            String path = props.getProperty("hdfs.path");
            String pathFormat = props.getProperty("hdfs.path.date.format");
            String zone = props.getProperty("hdfs.path.date.zone");
            Long windowTime = Long.valueOf(props.getProperty("window.time.second"));
            FlinkKafkaConsumer<String> flinkKafkaConsumer010 = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
            KeyedStream<TopicSource, String> KeyedStream = env.addSource(flinkKafkaConsumer010).map(FlinkParquetUtils::transformData).assignTimestampsAndWatermarks(new CustomWatermarks<TopicSource>()).keyBy(TopicSource::getId);

            DataStream<TopicSource> output = KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(windowTime))).apply(new WindowFunction<TopicSource, TopicSource, String, TimeWindow>() {
                /**
                 *
                 */
                private static final long serialVersionUID = 1L;

                @Override
                public void apply(String key, TimeWindow timeWindow, Iterable<TopicSource> iterable, Collector<TopicSource> collector) throws Exception {
                    iterable.forEach(collector::collect);
                }
            });

            // Send hdfs by parquet
            DateTimeBucketAssigner<TopicSource> bucketAssigner = new DateTimeBucketAssigner<>(pathFormat, ZoneId.of(zone));
//            StreamingFileSink<TopicSource> streamingFileSink = StreamingFileSink.forBulkFormat(new Path(path), ParquetAvroWriters.forReflectRecord(TopicSource.class)).withBucketAssigner(bucketAssigner).build();
//            output.addSink(streamingFileSink).name("Sink To HDFS");
            env.execute("TopicData");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static TopicSource transformData(String data) {
        if (data != null && !data.isEmpty()) {
            JSONObject value = JSON.parseObject(data);
            TopicSource topic = new TopicSource();
            topic.setId(value.getString("id"));
            topic.setTime(value.getLong("time"));
            return topic;
        } else {
            return new TopicSource();
        }
    }

    private static class CustomWatermarks<T> implements AssignerWithPunctuatedWatermarks<TopicSource> {

        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private Long cuurentTime = 0L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(TopicSource topic, long l) {
            return new Watermark(cuurentTime);
        }

        @Override
        public long extractTimestamp(TopicSource topic, long l) {
            Long time = topic.getTime();
            cuurentTime = Math.max(time, cuurentTime);
            return time;
        }
    }

    public static void main(String[] args) {
        getTopicToHdfsByParquet(env, props);
    }
}
