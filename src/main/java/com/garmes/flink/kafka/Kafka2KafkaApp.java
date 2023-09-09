package com.garmes.flink.kafka;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class Kafka2KafkaApp {

    public static void main(String[] args) throws Exception {

        String brokers = "localhost:9092";
        String topic = "flink";
        String consumerGroup = "flink2blob";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperty("partition.discovery.interval.ms", "10000")
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Printing to console what we have consumed
        //lines.print();

        DataStream<String> flatten =  lines.flatMap(new MyFlatMapFunction()).name("tokenizer").forward();

        // Before sending Kafka we need to seralize our value
        KafkaRecordSerializationSchema serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setTopic(topic + "-out")
                .build();

        // Adding KafkaSink
        KafkaSink kafkaSink = KafkaSink.builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(serializer)
                    .build();

        // Producing to kafka what we have just consumed. But to different topic.
        flatten.sinkTo(kafkaSink);

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
        env.execute("Kafka2Kafka");
    }

}
