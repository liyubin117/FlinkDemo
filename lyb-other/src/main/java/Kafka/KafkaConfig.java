package Kafka;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by liyubin on 2018/7/2 0002.
 */
public class KafkaConfig {
    public static final String HOSTNAME="localhost";
    public static final String ZOOKEEPER=HOSTNAME+":2188";
    public static final String KAFKA=HOSTNAME+":9092";
    public static final String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String ACKS = "sta.txt";
    public static final String GROUP_ID="test_group";
    public static final String TOPICS="test";

    public static List<String> getMsgList(){
        //ds取当前时间、role_id取随机值
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currTime = sdf.format(new Date());
        int role_id = new Random().nextInt(999999);
        List<String> msgList = Arrays.asList("["+currTime+"]"+"[Chat],{\"server\":1004,\"account_id\":\"aebfwvls2u3taspl@ad.netease.win.163.com\",\"old_accountid\":\"aebfwvls2u3taspl@ad.netease.win.163.com\",\"role_id\":"+role_id+",\"role_name\":\"杰超丶\",\"mac_addr\":\"08:00:27:8B:EF:5F\",\"chat_time\":1532880279,\"udid\":\"e6acd77f09b89d67\",\"content\":\"我改属性了\",\"channel\":5,\"scene\":\"ChatScene\",\"axis\":\"(0,0,0)\",\"y_account_id\":\"\",\"y_obj\":\"\",\"y_level\":50,\"y_name\":\"\"}");
        return msgList;
    }

}
