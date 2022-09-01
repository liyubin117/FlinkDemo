package org.lyb.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Utils {
    public static DefaultMQProducer generateProducer(String groupName, String nameSrvAddress) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(groupName);
        producer.setNamesrvAddr(nameSrvAddress);
        producer.start();
        return producer;
    }

    public static DefaultMQPushConsumer generateConsumer(String groupName, String nameSrvAddress, ConsumeFromWhere where, String topic, String tag) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setConsumeFromWhere(where);
        consumer.setNamesrvAddr(nameSrvAddress);
        //参数1：topic名字 参数2：tag名字
        consumer.subscribe(topic, tag);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgList) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动，会一直监听消息
        consumer.start();
        return consumer;
    }
}
