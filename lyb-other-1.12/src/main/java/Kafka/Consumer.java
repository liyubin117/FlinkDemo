//package Kafka;
//
//import kafka.consumer.Consumer;
//import kafka.consumer.ConsumerConfig;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
//import kafka.javaapi.consumer.ConsumerConnector;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
///**
// * Created by liyubin on 2018/7/3 0003.
// */
//public class Consumer implements Runnable{
//    private Properties properties;
//
//
//    public Consumer(String topic){
//        properties = new Properties();
//        properties.put("zookeeper.connect",KafkaConfig.ZOOKEEPER);
//        properties.put("group.id",KafkaConfig.GROUP_ID);
//
//        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
//        Map<String,Integer> topicCountMap = new HashMap<>();
//        topicCountMap.put(topic, 1);
//        //String: topic名， List：数据流
//        this.messageStream = consumer.createMessageStreams(topicCountMap);
//        this.stream = messageStream.get(topic).get(0);  //获取收到的topic数据流
//    }
//
//    @Override
//    public void run(){
//        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
//        while(iterator.hasNext()){
//            String message = new String(iterator.next().message());
//            System.out.println("recv: "+message);
//        }
//    }
//}
