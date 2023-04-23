package sink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 *   服务端
 *   kafka-topics.sh --create --replication-factor 1 --partitions 2 --topic flink-topic --zookeeper spark:2188
 *   kafka-topics.sh --create --replication-factor 1 --partitions 2 --topic flink-topic2 --zookeeper spark:2188
 *   kafka-console-producer.sh --broker-list spark:9992 --topic flink-topic
 *   kafka-console-consumer.sh --bootstrap-server spark:9992 --topic flink-topic
 *   kafka-console-consumer.sh --bootstrap-server spark:9992 --topic flink-topic2
 */

import java.util.Properties;
public class Kafka2Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "spark:9992");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.rest","latest");

        //flink作为kafaka消费者，消费flink-topic的数据
        FlinkKafkaConsumer<String> consumer = new
                FlinkKafkaConsumer<String>("flink-topic", new SimpleStringSchema(),
                properties);
        consumer.setStartFromGroupOffsets();
        DataStream<String> input = env.addSource(consumer);

        DataStream<String> result = input.filter(new FilterFunction<String>() {
            public boolean filter(String value) throws Exception {
                return value.contains("a");
            }
        });
        //flink作为kafka生产者，将flink-tipic中包含a的数据发给flink-topic2
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("spark:9992",
                "flink-topic2", new SimpleStringSchema());
        result.addSink(producer);

        result.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
