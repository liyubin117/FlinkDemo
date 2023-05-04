package Kafka;

import java.io.IOException;
import java.util.Properties;
import org.apache.commons.lang.SystemUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SaslProducer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "sloth-test0.dg.163.org:9992");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        System.setProperty(
                "java.security.auth.login.config",
                SystemUtils.getUserDir() + "/lyb-other/src/main/resources/kafka_client_sasl.conf");

        Producer<String, String> producer = new KafkaProducer<>(props);
        System.out.println("send msg");
        producer.send(new ProducerRecord<String, String>("topic0620", "123123123"));
        System.out.println("send one msg : 123123123");
        producer.close();

        //        Consumer<String, String> consumer = new KafkaConsumer();
    }
}
