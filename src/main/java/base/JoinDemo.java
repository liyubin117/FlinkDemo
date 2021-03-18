package base;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class JoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Integer>> left = env.socketTextStream("localhost", 9888)
                .map(new RichMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new Tuple2<>(arr[0], Integer.valueOf(arr[1]));
                    }
                })
                ;
        DataStream<Tuple2<String, Integer>> right = env.socketTextStream("localhost", 9889)
                .map(new RichMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new Tuple2<>(arr[0], Integer.valueOf(arr[1]));
                    }
                })
                ;
        left.print("left");
        right.print("right");
        left.join(right)
            .where(new MySelector())
            .equalTo(new MySelector())
            .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
            .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Object>() {
                @Override
                public Object join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                    return first.f1 + second.f1;
                }
            })
            .print("result");

        env.execute();
    }
}

class MySelector implements KeySelector<Tuple2<String, Integer>, String>{
    @Override
    public String getKey(Tuple2<String, Integer> value) throws Exception {
        return value.f0;
    }
}
