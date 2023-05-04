package org.lyb.rocketmq;

import static org.lyb.rocketmq.Utils.generateConsumer;
import static org.lyb.rocketmq.Utils.generateProducer;

import java.io.File;
import java.net.URISyntaxException;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class RocketmqE2eTest {
    private static final String DOCKER_IMAGE_NAME = "apache/rocketmq:4.9.4";
    private static GenericContainer<?> container =
            new GenericContainer<>(DockerImageName.parse(DOCKER_IMAGE_NAME)).withExposedPorts(9876);
    DockerComposeContainer composeContainer =
            new DockerComposeContainer<>(
                    new File(
                            RocketmqE2eTest.class
                                    .getClassLoader()
                                    .getResource("docker-compose.yaml")
                                    .toURI()));

    public static final String TOPIC = "topic1";
    public static final String TAG = "tag1";
    private String nameSrvAddress;

    public RocketmqE2eTest() throws URISyntaxException {}

    @Before
    public void setUp() {
        container.start();
        nameSrvAddress = container.getHost() + ":" + container.getFirstMappedPort();
    }

    @Test
    public void test1()
            throws MQClientException, MQBrokerException, RemotingException, InterruptedException {

        DefaultMQProducer producer = generateProducer("group1", nameSrvAddress);
        producer.send(new Message(TOPIC, TAG, "a message".getBytes()));
        producer.shutdown();
        DefaultMQPushConsumer consumer =
                generateConsumer(
                        "group1",
                        nameSrvAddress,
                        ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
                        TOPIC,
                        TAG);
    }
}
