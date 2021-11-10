package Kafka;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by liyubin on 2018/7/2 0002.
 */
public class Producer implements Runnable{
    private Properties properties;
    private String topic;
    private org.apache.kafka.clients.producer.Producer producer;
    private int interval;

    public Producer(String topic, int interval){
        properties = new Properties();
        properties.put("bootstrap.servers", KafkaConfig.KAFKA);
        properties.put("key.serializer", KafkaConfig.SERIALIZER);
        properties.put("value.serializer", KafkaConfig.SERIALIZER);
//        properties.put("request.required.acks", KafkaConfig.ACKS);
        this.topic = topic;
        producer = new KafkaProducer<Integer,String>(properties);
        this.interval = interval;
    }

    @Override
    public void run(){
        int i=0;
        while(true){
            List<String> msgList = KafkaConfig.getMsgList();
            msgList = Arrays.asList("{\"userid\":90300,\"text\":\"2021-10-11\", \"action\":\"10:20:30\", \"ts_str\":\"2021-11-05 10:20:30\", \"appver\":\"v2.0\", \"ts\":\"2021-11-05 10:20:30\"}");
            for(String msg:msgList){
                producer.send(new ProducerRecord(this.topic, msg));
            }

            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
