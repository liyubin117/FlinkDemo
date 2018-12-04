package sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import org.apache.flink.util.ExceptionUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import scopt.Check;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsSink{
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStream<String> input = env.socketTextStream("my",9888);

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("my", 9200, "http"));

// use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);

                        return Requests.indexRequest()
                                .index("my-index")
                                .type("my-type")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        //批量写入
        //批量写入es时的最大记录条数，设为1表示来一条就写一条
        esSinkBuilder.setBulkFlushMaxActions(1);
        //批量写入es时的最大数据量（单位是MB）
        esSinkBuilder.setBulkFlushMaxSizeMb(10);
        //批量写入es时的时间间隔（单位是毫秒），设置后无视前两条配置，只按时间间隔批量写入
        esSinkBuilder.setBulkFlushInterval(2000);

        //重试机制
        //开启重试机制（必须在开启flink检查点的情况下才有效）
        env.enableCheckpointing(2000);
        esSinkBuilder.setBulkFlushBackoff(true);
        //多次重试之间的时间间隔，CONSTANT为固定常数，如2 -> 2 -> 2，EXPONENTIAL为指数增长，2 -> 4 -> 8
        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
        //多次重试之间的固定或基数时间间隔（单位是毫秒）
        esSinkBuilder.setBulkFlushBackoffDelay(2000);
        //最大重试次数
        esSinkBuilder.setBulkFlushBackoffRetries(50);

        //失败处理机制，传入实现了ActionRequestFailureHandler接口的类。此处即把因节点请求队列满而失败的请求重新加入队列，其他失败则抛出异常
        //不能简单地用try-catch捕捉异常
//        esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler() {
//            @Override
//            public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
//                if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
//                    indexer.add(action);
//                } else if (ExceptionUtils.findThrowable(failure, ResponseException.class).isPresent()){
//                    indexer.add(action);
//                } else {
//                    indexer.add(action);
//                    //throw failure;
//                }
//            }});
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

        //配置REST客户端
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    //添加请求头部信息
                    restClientBuilder.setDefaultHeaders(new Header[]{new BasicHeader("k1","v1")});
                    restClientBuilder.setMaxRetryTimeoutMillis(2000);
                }
        );

        // finally, build and add the sink to the job's pipeline
        input.addSink(esSinkBuilder.build());
        env.execute("socket to es");
    }
}
