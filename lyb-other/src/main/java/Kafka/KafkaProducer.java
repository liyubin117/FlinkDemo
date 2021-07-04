package Kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.List;
import java.util.Properties;

/**
 * Created by liyubin on 2018/7/2 0002.
 */
public class KafkaProducer implements Runnable{
    private Properties properties;
    private String topic;
    private Producer producer;
    private int interval;

    public KafkaProducer(String topic, int interval){
        properties = new Properties();
        properties.put("metadata.broker.list", KafkaConfig.KAFKA);
        properties.put("serializer.class", KafkaConfig.SERIALIZER);
        properties.put("request.required.acks", KafkaConfig.ACKS);
        this.topic = topic;
        producer = new Producer<Integer,String>(new ProducerConfig(properties));
        this.interval = interval;
    }

    @Override
    public void run(){
        int i=0;
        while(true){
            List<String> msgList = KafkaConfig.getMsgList();
            for(String msg:msgList){
                producer.send(new KeyedMessage<Integer,String>(this.topic, msg));
            }

            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
