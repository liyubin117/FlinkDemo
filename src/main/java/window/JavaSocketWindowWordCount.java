package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class JavaSocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port",9888);
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("spark", port, "\n");

        DataStream<WordWithCount> windowCounts = text
                .flatMap(new MyFlatMapFunction())
                .keyBy("word")
                //.timeWindow(Time.seconds(5), Time.seconds(1))  //若去掉window，则自动计算累积值
                .reduce(new MyReduceFunction());

        windowCounts.print().setParallelism(3);

        env.execute("Socket Window WordCount");
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, WordWithCount> {
        public void flatMap(String in, Collector<WordWithCount> out) {
            for (String word: in.split("\\s")) {
                out.collect(new WordWithCount(word, 1L));
            }
        }
    }

    public static class MyReduceFunction implements ReduceFunction<WordWithCount> {
        public WordWithCount reduce(WordWithCount a, WordWithCount b) {
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
