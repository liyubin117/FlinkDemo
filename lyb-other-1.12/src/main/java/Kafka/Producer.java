package Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by liyubin on 2018/7/2 0002.
 */
public class Producer implements Runnable{
    private static final Properties properties;
    private final String topic;
    private static final org.apache.kafka.clients.producer.Producer<Integer, String> producer;
    private int interval;

    static {
        properties = new Properties();
        properties.put("bootstrap.servers", KafkaConfig.KAFKA);
        properties.put("key.serializer", KafkaConfig.SERIALIZER);
        properties.put("value.serializer", KafkaConfig.SERIALIZER);
        producer = new KafkaProducer<>(properties);
    }

    public Producer(String topic, int interval){
//        properties.put("request.required.acks", KafkaConfig.ACKS);
        this.topic = topic;
        this.interval = interval;
    }

    public Producer(String topic) {
        this.topic = topic;
    }

    public void sendMessage(String msg) {
        producer.send(new ProducerRecord<>(this.topic, msg));
    }

    @Override
    public void run(){
        int i=0;
        while(true){
            List<String> msgList = KafkaConfig.getMsgList();
            msgList = Collections.singletonList("{\"userid\":90300,\"text\":\"2021-10-11\", \"action\":\"10:20:30\", \"ts_str\":\"2021-11-05 10:20:30\", \"appver\":\"v2.0\", \"ts\":\"2021-11-05 10:20:30\"}");
            for(String msg:msgList){
                producer.send(new ProducerRecord<>(this.topic, msg));
            }

            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
