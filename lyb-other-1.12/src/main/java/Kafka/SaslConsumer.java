package Kafka;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class SaslConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "sloth-test0.dg.163.org:9992");
        // group 代表一个消费组
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "600000");
        props.put("zookeeper.sync.time.ms", "200000");
        props.put("auto.commit.interval.ms", "100000");
        props.put(
                org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(
                org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                1000 + ""); // 设置最大消费数
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");

        props.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"alice-pwd\";");

        Consumer consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("topic0620"));

        System.out.println(consumer.listTopics());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf(
                        "offset = %d, key = %s, value = %s",
                        record.offset(), record.key(), record.value() + "\n");
        }
    }
}
