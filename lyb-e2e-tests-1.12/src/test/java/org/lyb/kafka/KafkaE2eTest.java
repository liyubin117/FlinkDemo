package org.lyb.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


public class KafkaE2eTest {
    public static final String DOCKER_IMAGE_NAME = "confluentinc/cp-kafka:5.4.3";
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(DOCKER_IMAGE_NAME));
    public static final KafkaContainerClient client = new KafkaContainerClient(kafkaContainer);
    public static final String TOPIC = "topic1";

    @Before
    public void setUp() {
        kafkaContainer.start();
        client.createTopic(1, 2, TOPIC);
    }
    
    @Test
    public void test1() throws Exception {
        // 生产消息
        for (int i = 0; i < 10; i++) {
            client.sendMessages(TOPIC, new StringSerializer(), "lyb-" + i);
        }
        
        client.readMessages(10, "group1", TOPIC, new StringDeserializer())
                .forEach(System.out::println);
    }

}