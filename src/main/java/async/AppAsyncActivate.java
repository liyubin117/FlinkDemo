//package async;
//
//import com.alibaba.fastjson.JSONObject;
//import com.dianhun.flinkstreaming.KafkaTableSourceConfigKeys;
//import com.dianhun.pojo.AppStartLog;
//import com.dianhun.redis.common.AsyncRedisUtil;
//import com.dianhun.redis.common.RedisInfo;
//import com.dianhun.utils.KafkaUtil;
//import io.lettuce.core.KeyValue;
//import io.lettuce.core.RedisFuture;
//import io.lettuce.core.api.async.RedisKeyAsyncCommands;
//import io.lettuce.core.api.async.RedisStringAsyncCommands;
//import org.apache.commons.codec.digest.DigestUtils;
//import org.apache.commons.collections4.IterableUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple1;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.Preconditions;
//import org.joda.time.DateTime;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//
///**
// * AppAsyncActivate
// *
// * @author GeZhiHui
// * @create 2018-12-19
// **/
//
//public class AppAsyncActivate {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(AppActivate.class);
//    private static final String CHECKPOINT_DATA_URI = "hdfs://dhcluster/user/flink/checkpoint/";
//
//    private static final String INSERT_STRING = "INSERT INTO app_activate(cur_date,start_time,product_id,dev_os,activate_type,activate_count) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE cur_date=VALUES(cur_date),start_time=VALUES(start_time),product_id=VALUES(product_id),dev_os=VALUES(dev_os),activate_type=VALUES(activate_type),activate_count=VALUES(activate_count)";
//    private static final String CONNECT_INFO = "jdbc:mysql://127.0.0.1:3306/flink_test";
//    private static final String USERNAME = "root";
//    private static final String PD = "root";
//
//    private static final long TIMEOUT = 10000;
//    private static final int CAPACITY = 100;
//
//    public static void main(String[] args) throws Exception {
//        final int checkpointInterval = 30000;
//        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
//
//        String jobName = parameterTool.get(KafkaTableSourceConfigKeys.JOB_NAME);
//        String bootstrapServers = parameterTool.get(KafkaTableSourceConfigKeys.BOOTSTRAP_SERVERS);
//        String groupId = parameterTool.get(KafkaTableSourceConfigKeys.GROUP_ID);
//        int parallelism = parameterTool.getInt(KafkaTableSourceConfigKeys.PARALLELISM, 1);
//        String offset = parameterTool.get(KafkaTableSourceConfigKeys.OFFSET);
//
//        Preconditions.checkNotNull(jobName, "Must provide --job.name argument");
//        Preconditions.checkNotNull(bootstrapServers, "Must provide --bootstrap.servers argument");
//        Preconditions.checkNotNull(groupId, "Must provide --group.id argument");
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().enableSysoutLogging();
//        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
//        env.enableCheckpointing(checkpointInterval);
//        env.getConfig().setGlobalJobParameters(parameterTool);
//        CheckpointConfig config = env.getCheckpointConfig();
//        //config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // env.setStateBackend(new FsStateBackend(CHECKPOINT_DATA_URI + jobName));
//        env.setParallelism(parallelism);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//
//        String topic = "dh_ods_ods_dh_appstart";
//
//        Properties properties = new Properties();
//        properties.put(KafkaTableSourceConfigKeys.BOOTSTRAP_SERVERS, bootstrapServers);
//        properties.put(KafkaTableSourceConfigKeys.GROUP_ID, groupId);
//        FlinkKafkaConsumer09<String> kafkaConsumer09 = new FlinkKafkaConsumer09<>(topic, new SimpleStringSchema(), properties);
//        KafkaUtil.setOffsets(offset, topic, kafkaConsumer09);
//        TypeInformation<Tuple2<String, AppStartLog>> typeInformation = TypeInformation.of(new TypeHint<Tuple2<String, AppStartLog>>() {
//        });
//        DataStream<String> dataStream = env.addSource(kafkaConsumer09, jobName);
//        DataStream<AppStartLog> inputStream = dataStream
//                .map((MapFunction<String, AppStartLog>) value -> {
//                    AppStartLog appStartLog = new AppStartLog();
//                    if (StringUtils.isNotBlank(value)) {
//                        appStartLog = JSONObject.parseObject(value, AppStartLog.class);
//                    }
//                    return appStartLog;
//                })
//                .filter((FilterFunction<AppStartLog>) value -> {
//                    String startType = value.getStartType();
//                    return StringUtils.equalsIgnoreCase(startType, "1");
//                });
//        DataStream<AppStartLog> outStream = AsyncDataStream.orderedWait(inputStream, new RedisRichAsyncFunction(), TIMEOUT, TimeUnit.MILLISECONDS, CAPACITY);
//        DataStream<Map<String, String>> result = outStream.map((MapFunction<AppStartLog, Tuple2<String, AppStartLog>>) value -> new Tuple2<>(value.getIsExtend() + "_" + value.getDevOs() + "_" + value.getProductId(), value))
//                .returns(typeInformation)
//                .keyBy(0)
//                .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.minutes(1), Time.hours(-8)))
//                .process(new ProcessWindowFunction<Tuple2<String, AppStartLog>, Map<String, String>, Tuple, TimeWindow>() {
//                    private static final long serialVersionUID = 1682977400662875760L;
//
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, AppStartLog>> elements, Collector<Map<String, String>> out) throws Exception {
//                        String key = ((Tuple1<String>) tuple).f0;
//                        String[] s = StringUtils.split(key, "_");
//                        TimeWindow window = context.window();
//                        long start = window.getStart();
//                        Map<String, String> record = new HashMap<>();
//                        record.put("cur_date", new DateTime(start).toString("yyyy-MM-dd"));
//                        record.put("start_time", new DateTime(start).toString("yyyy-MM-dd HH:mm:ss"));
//                        record.put("activate_type", s[0]);
//                        record.put("dev_os", s[1]);
//                        record.put("product_id", s[2]);
//                        record.put("activate_count", String.valueOf(IterableUtils.toList(elements).size()));
//                        out.collect(record);
//                    }
//                });
//
//        //result.addSink(new MySQLSinkForMap(INSERT_STRING, CONNECT_INFO, USERNAME, PD)).name("MySQLSink");
//        result.print().name("print");
//        env.execute(jobName);
//    }
//
//
//    public static class RedisRichAsyncFunction extends RichAsyncFunction<AppStartLog, AppStartLog> {
//
//        RedisKeyAsyncCommands<String, String> async;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//            RedisInfo redisInfo = new RedisInfo();
//            redisInfo.setRedisType("1");
//            redisInfo.setMaxIdle("100");
//            redisInfo.setMinIdle("0");
//            redisInfo.setMaxTotal("500");
//            redisInfo.setTimeout(1000);
//            redisInfo.setUrl("127.0.0.1:6379");
//            redisInfo.setDatabase("0");
//            async = AsyncRedisUtil.getRedisKeyAsyncCommands(redisInfo);
//        }
//
//        @Override
//        public void asyncInvoke(AppStartLog input, AsyncCollector<AppStartLog> collector) throws Exception {
//            String devOs = input.getDevOs();
//            String devImei = input.getDevImei();
//            String key = "default";
//            if (StringUtils.equalsIgnoreCase("android", devOs)) {
//                key = DigestUtils.md5Hex(devImei) + "_" + input.getProductId();
//
//            }
//            if (StringUtils.equalsIgnoreCase("ios", devOs)) {
//                key = DigestUtils.md5Hex(devImei).toUpperCase() + "_" + input.getProductId();
//            }
//            List<String> value = async.keys(key).get();
//            String[] values = value.toArray(new String[value.size()]);
//            if (value.isEmpty()) {
//                input.setIsExtend("0");
//            } else {
//                RedisFuture<List<KeyValue<String, String>>> future = ((RedisStringAsyncCommands) async).mget(values);
//                future.thenAccept(keyValues -> {
//                    if (!keyValues.isEmpty()) {
//                        input.setIsExtend("1");
//                    } else {
//                        input.setIsExtend("0");
//                    }
//                });
//            }
//            collector.collect(Collections.singleton(input));
//
//        }
//
//        @Override
//        public void close() throws Exception {
//            super.close();
//            AsyncRedisUtil.close();
//        }
//    }
//
//}
