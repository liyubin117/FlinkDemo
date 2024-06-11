package window;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class JavaSocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port", 9888);
        } catch (Exception e) {
            System.err.println(
                    "No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", port, "\n");

        DataStream<WordWithCount> windowCounts =
                text.map(
                                new RichMapFunction<String, String>() {
                                    private transient Counter counter;

                                    @Override
                                    public void open(Configuration config) {
                                        this.counter =
                                                getRuntimeContext()
                                                        .getMetricGroup()
                                                        .counter("myCounter");
                                    }

                                    @Override
                                    public String map(String str) throws Exception {
                                        counter.inc();
                                        return str;
                                    }
                                })
                        .flatMap(new MyFlatMapFunction())
                        .keyBy("word")
                        // .timeWindow(Time.seconds(5), Time.seconds(1))  //若去掉window，则自动计算累积值
                        .reduce(new MyReduceFunction());

        windowCounts.print();
        env.execute("Socket Window WordCount");
    }

    public static class MyFlatMapFunction extends RichFlatMapFunction<String, WordWithCount> {
        private transient int valueToExpose = 0;

        @Override
        public void open(Configuration config) {
            getRuntimeContext()
                    .getMetricGroup()
                    .gauge(
                            "MyFlatMapGauge",
                            new Gauge<Integer>() {
                                @Override
                                public Integer getValue() {
                                    return valueToExpose;
                                }
                            });
        }

        public void flatMap(String in, Collector<WordWithCount> out) {
            valueToExpose++;
            for (String word : in.split("\\s")) {
                out.collect(new WordWithCount(word, 1L));
            }
        }
    }

    public static class MyReduceFunction extends RichReduceFunction<WordWithCount> {
        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            this.counter = getRuntimeContext().getMetricGroup().counter("myCounter");
        }

        public WordWithCount reduce(WordWithCount a, WordWithCount b) {
            counter.inc();
            return new WordWithCount(a.word, a.count + b.count);
        }
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
