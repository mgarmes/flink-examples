package com.garmes.flink.kafka;

import java.time.Duration;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;


public class Kafka2FileApp {

    public static void main(String[] args) throws Exception {

        String brokers = "localhost:9092";
        String topic = "flink";
        String consumerGroup = "flink2blob";

        Path outputPath =  new Path("/tmp/flink2file");
        System.out.println("outputPath: " +  outputPath.toUri().toString());
        System.out.println("getFileSystem: " + outputPath.getFileSystem().getUri().toString());

        // -------------------------- config -------------------------------- //
        // set up the execution environment
        //final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(Duration.ofSeconds(10).toMillis(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints/");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // enables the unaligned checkpoints
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        env.setParallelism(2);

        // -------------------------- job -------------------------------- //
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperty("partition.discovery.interval.ms", "10000")
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<String> flatten =  lines.flatMap(new MyFlatMapFunction()).name("tokenizer").forward();

        //flatten.print();

        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();

        FileSink<String> sink = FileSink.<String>forRowFormat(outputPath, new SimpleStringEncoder<String>())
                .withOutputFileConfig(config)
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofSeconds(10))
                        .withInactivityInterval(Duration.ofSeconds(1))
                        .withMaxPartSize(MemorySize.ofMebiBytes(1))
                        .build()
                )
                .build();

        flatten.sinkTo(sink).name("file-sink");

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
        env.execute("flatten");
    }

}
