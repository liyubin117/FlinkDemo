package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * 服务端
 * create 't2','f1','f2'
 * 输入数据格式（行键|列族,列,值:列族,值,...）
 * rk1|f1,c1,v1:f2,c2,v2
 */
public class TestHBaseSqlSink {
    public TestHBaseSqlSink(){}

    @Test
    public void test(){
        //hbase连接信息
        Properties p = new Properties();
        p.setProperty("hbase.zookeeper.property.clientPort", "2188");
        p.setProperty("hbase.zookeeper.quorum", "my");
        HBaseSink hbaseSink = new HBaseSink<ArrayList<ArrayList<String>>>("t2",p){
            @Override
            public void invoke(ArrayList<ArrayList<String>> value) throws Exception{
                try {
                    String rowKey = value.get(0).get(0);
                    if(rowKey != "!!!error input!!!"){
                        value.remove(0);
                        Put put = new Put(Bytes.toBytes(rowKey));
                        for(int i=0;i<value.size();i++){
                            ArrayList<String> subList = value.get(i);
                            int size = subList.size();
                            if(3==size){
                                put.addColumn(Bytes.toBytes(subList.get(0).toString())
                                        ,Bytes.toBytes(subList.get(1).toString())
                                        ,Bytes.toBytes(subList.get(2).toString()));
                            }else if(2==size) {
                                put.addColumn(Bytes.toBytes(subList.get(0).toString())
                                        , Bytes.toBytes("")
                                        , Bytes.toBytes(subList.get(1).toString()));
                            }else{
                                getLogger().error("+++wrong format!+++");
                                return;
                            }
                        }
                        getTable().put(put);
                        getLogger().info("+++a row has been put successfully!+++");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> socketStream = env.socketTextStream("my",9888);
        DataStream<ArrayList<ArrayList<String>>> text = socketStream.map(new MapFunction<String, ArrayList<ArrayList<String>>>() {
            @Override
            public ArrayList<ArrayList<String>> map(String s) throws Exception {
                //可以加一些必要的字段分隔、字段校验
                String[] cols = s.split("\\|");
                if (cols.length == 2){
                    ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
                    ArrayList<String> subList = new ArrayList<String>();
                    subList.add(cols[0]);
                    list.add(subList);
                    String[] v = cols[1].split(":");
                    for(int i=0;i<v.length;i++){
                        ArrayList<String> l = new ArrayList<>();
                        String[] v2 = v[i].split(",");
                        for(int m=0;m<v2.length;m++){
                            l.add(v2[m]);
                        }
                        list.add(l);
                    }
                    return list;
                }else{
                    System.out.println("!!!error input!!!");
                    ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
                    ArrayList<String> subList = new ArrayList<String>();
                    subList.add("!!!error input!!!");
                    list.add(subList);
                    return list;
                }
            }
        });
        text.addSink(hbaseSink);
        try {
            env.execute("text to hbase start");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
