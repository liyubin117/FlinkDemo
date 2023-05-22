package org.lyb.cep;

import java.time.Duration;
import java.util.function.Consumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.cep.CEP;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.lyb.cep.patterns.MatcherV3;
import org.lyb.cep.patterns.PatternMatcher;
import org.lyb.cep.records.SensorReading;
import org.lyb.cep.records.SensorReadingDeserializationSchema;

public class PatternMatchingCEP {

    static final String TOPIC = "input";

    public static void main(String[] args) throws Exception {
        Duration limitOfHeatTolerance = Duration.ofSeconds(2);
        PatternMatcher<SensorReading, SensorReading> matcher = new MatcherV3();
        runJob(matcher, limitOfHeatTolerance);
    }

    static void runJob(
            PatternMatcher<SensorReading, SensorReading> patternMatcher,
            Duration limitOfHeatTolerance)
            throws Exception {
        KafkaSource<SensorReading> source =
                KafkaSource.<SensorReading>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SensorReadingDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        defineWorkflow(
                env,
                source,
                patternMatcher,
                limitOfHeatTolerance,
                workflow -> workflow.sinkTo(new PrintSink<>("ALERT", true)),
                workflow -> workflow.sinkTo(new PrintSink<>()));

        env.execute();
    }

    static void defineWorkflow(
            StreamExecutionEnvironment env,
            Source<SensorReading, ?, ?> source,
            PatternMatcher<SensorReading, SensorReading> patternMatcher,
            Duration limitOfHeatTolerance,
            Consumer<DataStream<SensorReading>> alertSinkApplier,
            Consumer<DataStream<SensorReading>> eventSinkApplier) {

        final WatermarkStrategy<SensorReading> watermarking =
                WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.timestamp.toEpochMilli())
                        .withIdleness(Duration.ofSeconds(1));

        final KeyedStream<SensorReading, Long> events =
                env.fromSource(source, watermarking, "Kafka").keyBy(e -> e.deviceId);

        DataStream<SensorReading> matched =
                CEP.pattern(events, patternMatcher.pattern(limitOfHeatTolerance))
                        .inEventTime()
                        .process(patternMatcher.process());

        alertSinkApplier.accept(matched);
        eventSinkApplier.accept(events);
    }
}
