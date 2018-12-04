package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class TestMySqlSink {
    public TestMySqlSink(){}

    @Test
    public void test(){
        //mysql连接信息
        MySqlSink mysqlSink = new MySqlSink("com.mysql.jdbc.Driver"
                ,"jdbc:mysql://spark:3306/test2"
                ,"root"
                ,"sbpgfsse"
                ,"INSERT into device(device_id,device_no,name,gender) values(?,?,?,?) " +
                        "ON DUPLICATE KEY UPDATE device_no = VALUES(device_no),name = VALUES(name),gender = VALUES(gender)");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStream<Tuple4<String, Integer, String, String>> text = env.fromElements(new Tuple4("device1",1,"liyubin","male"));
        DataStream<String> socketStream = env.socketTextStream("spark",9888);
        DataStream<Tuple4<String, Integer, String, String>> text = socketStream.map(new MapFunction<String, Tuple4<String, Integer, String, String>>() {
            @Override
            public Tuple4<String, Integer, String, String> map(String s) throws Exception {
                //可以加一些必要的字段分隔、字段校验
                String[] cols = s.split("\\s");
                if (cols.length == 4){
                    return new Tuple4(String.valueOf(cols[0]),Integer.valueOf(cols[1]),String.valueOf(cols[2]),String.valueOf(cols[3]));
                }else{
                    System.out.println("!!!error input!!!");
                    return new Tuple4("!!!error input!!!",Integer.valueOf(-1),"error input","error input");
                }
            }
        });
        text.addSink(mysqlSink);
        try {
            env.execute("text to mysql start");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
