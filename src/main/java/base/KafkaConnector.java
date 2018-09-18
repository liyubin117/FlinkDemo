package base;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

import java.util.Properties;
public class KafkaConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka-topics.sh --create --replication-factor 1 --partitions 2 --topic flink-topic --zookeeper spark:2181
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "spark:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.rest","latest");

        //flink作为kafaka消费者
        FlinkKafkaConsumer09<String> consumer = new
                FlinkKafkaConsumer09<String>("flink-topic", new SimpleStringSchema(),
                properties);
        consumer.setStartFromGroupOffsets();
        DataStream<String> input = env.addSource(consumer);

        DataStream<String> result = input.filter(new FilterFunction<String>() {
            public boolean filter(String value) throws Exception {
                return value.contains("a");
            }
        });
        //flink作为kafka生产者
        FlinkKafkaProducer09<String> producer = new FlinkKafkaProducer09<String>("spark:9092",
                "flink-topic", new SimpleStringSchema());
        result.addSink(producer);

        result.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
