package sink;////在1.12.0无法使用
//
//package sink;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
///*在1.12.0已经不能用
//import org.apache.flink.streaming.connectors.fs.StringWriter;
//import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
//import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;*/
//
//import org.apache.flink.connector.file.sink
//import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
//import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
//
//
//public class HdfsSink {
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
//
//        //DataStream<Tuple4<String, Integer, String, String>> text = env.fromElements(new Tuple4("device1",1,"liyubin","male"));
//        DataStream<String> socketStream = env.socketTextStream("my",9888);
//
//        BucketingSink<String> hdfsSink = new BucketingSink<>("hdfs://my:9000/flink/file1");
//        //某天的元素放在同一个桶里，并体现在文件名上，默认是yyyy-MM-dd - HH，即某小时的元素放在同一个桶里
//        hdfsSink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd"));
//        //定义序列化器，将String转化成UTF-8格式的字节
//        hdfsSink.setWriter(new StringWriter<>());
//        //设置桶part文件最大字节数为2k，桶part文件后缀若为in-progress表示还在生成中，pending表示已生成。若超过此值则新建一个桶part文件
//        hdfsSink.setBatchSize(2*1024);
//        //当桶变得不活动时，打开的part文件将被刷新并关闭。一个桶在最近没有被写入时被视为非活动的。默认情况下，接收器每分钟检查非活动的桶，并关闭一分钟内尚未写入的任何桶文件
//        //设置的是检查桶是否活跃的时间间隔（单位是毫秒）
//        hdfsSink.setInactiveBucketCheckInterval(1000L);
//        //设置的是桶不活跃时长的阈值,多久时间没有数据写入就关闭桶（单位是毫秒）
//        hdfsSink.setInactiveBucketThreshold(1000L);
//
//        socketStream.addSink(hdfsSink);
//        env.execute("socket to hdfs");
//    }
//}
