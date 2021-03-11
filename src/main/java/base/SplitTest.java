////在1.12.0无法使用
//
//package base;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SplitStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.*;
//
//public class SplitTest {
//    public static void main(String[] args) {
//        Map<String,List<List<String>>> db = new HashMap<>();
//        Map<String,List<List<String>>> index = new HashMap<>();
//        Map<String,List<List<String>>> error = new HashMap<>();
//
//        List<List<String>> actions = new ArrayList<>();
//        actions.add(Arrays.asList("c1,v1".split(",")));
//        actions.add(Arrays.asList("c2,v2".split(",")));
//
//        List<List<String>> actions2 = new ArrayList<>();
//        actions2.add(Arrays.asList("c10,v10".split(",")));
//        actions2.add(Arrays.asList("c20,v20".split(",")));
//
//        List<List<String>> actions3 = new ArrayList<>();
//        actions3.add(Arrays.asList("c100,v100".split(",")));
//        actions3.add(Arrays.asList("c200,v200".split(",")));
//
//        db.put("sink",Arrays.asList(Arrays.asList("db")));
//        db.put("actions",actions);
//
//        index.put("sink",Arrays.asList(Arrays.asList("index")));
//        index.put("actions",actions2);
//
//        error.put("sink",Arrays.asList(Arrays.asList("error")));
//        error.put("actions",actions3);
//
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Map<String,List<List<String>>>> input = env.fromElements(db,index,error);
//        SplitStream<Map<String,List<List<String>>>> splitStream = input.split(new OutputSelector<Map<String, List<List<String>>>>() {
//                    @Override
//                    public Iterable<String> select(Map<String, List<List<String>>> value) {
//                        List<String> output = new ArrayList<>();
//                        String key = value.get("sink").get(0).get(0);
//                        if(key.equals("db")){
//                            output.add("db");
//                        }else if(key.equals("index")){
//                            output.add("index");
//                        }else{
//                            output.add("error");
//                        }
//                        return output;
//                    }
//                });
//
//        splitStream.select("db","index")
//                .map(new MapFunction<Map<String, List<List<String>>>, List<List<String>>>() {
//                         @Override
//                         public List<List<String>> map(Map<String, List<List<String>>> value) throws Exception {
//                             return value.get("actions");
//                         }
//                     }).print();
//        splitStream.select("error")
//                .map(new MapFunction<Map<String, List<List<String>>>, List<List<String>>>() {
//                    @Override
//                    public List<List<String>> map(Map<String, List<List<String>>> value) throws Exception {
//                        value.get("actions").add(Arrays.asList("!!!!!"));
//                        return value.get("actions");
//                    }
//                }).print();
//
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
