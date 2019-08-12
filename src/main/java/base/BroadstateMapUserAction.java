package base;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
nc -lk my 9888
100,a
200,b
100,b
200,a
nc -lk my 9889
[{"patternId":100,"firstAction":"a","secondAction":"b"},{"patternId":200,"firstAction":"c","secondAction":"d"},{"patternId":0,"firstAction":"a","secondAction":"c"}]
 */
public class BroadstateMapUserAction {
    /* 创建 Broadcast State 的描述 MapStateDescriptor，参数类型第一个是键类型，第二个是值类型 */
    private static final MapStateDescriptor<Void, Map<Integer,Pattern>> bcStateDescriptor =
            new MapStateDescriptor<>(
                    "patterns",
                    Types.VOID,
                    Types.MAP(Types.INT, Types.POJO(Pattern.class))
            );

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Action> actions = env.socketTextStream("my",9888)
                .filter((String str) -> str.split(",").length==2)
                .map((String str) -> new Action(Integer.valueOf(str.split(",")[0]),str.split(",")[1]));
        DataStream<Map<Integer,Pattern>> patterns = env.socketTextStream("my",9889)
                .map(new MapFunction<String, Map<Integer, Pattern>>() {
                    @Override
                    public Map<Integer, Pattern> map(String str) throws Exception {
                        HashMap<Integer, Pattern> map = new HashMap<>();
                        JSONArray arr = JSON.parseArray(str);
                        for (int i = 0; i < arr.size(); i++) {
                            JSONObject obj = (JSONObject) arr.get(i);
                            map.put(obj.getInteger("patternId"), new Pattern(obj.getInteger("patternId"), obj.getString("firstAction"), obj.getString("secondAction")));
                        }
                        return map;
                    }
                });
        /* 设置Key的字段，同样用户的数据分到相同的operator里 */
        KeyedStream<Action, Integer> actionsByUser = actions.keyBy(
                (KeySelector<Action, Integer>) action -> action.getUserId()
        );

        /* 定制 Broadcast Stream */
        BroadcastStream<Map<Integer,Pattern>> bcedPatterns = patterns.broadcast(bcStateDescriptor);
        /* 连接两个流并应用规则 */
        DataStream<Tuple2<Integer,Pattern>> matches = actionsByUser.connect(bcedPatterns)
                .process(
                        //参数依次是键、主数据流（Action）、广播流（Pattern）、返回结果
                        new KeyedBroadcastProcessFunction<Integer, Action, Map<Integer,Pattern>, Tuple2<Integer, Pattern>>() {
                            //当前用户的最近状态
                            ValueState<String> prevActionState;

                            @Override
                            public void open(Configuration conf){
                                //初始化最近状态
                                prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAction",Types.STRING));
                            }

                            /* 被主数据流里的每个元素调用，最近状态与当前状态结合起来，检查是否符合广播流里的模式 */
                            @Override
                            public void processElement(Action action, ReadOnlyContext ctx, Collector<Tuple2<Integer, Pattern>> out) throws Exception {
                                //得到当前模式
                                Map<Integer,Pattern> map = ctx.getBroadcastState(bcStateDescriptor).get(null);
                                if(map==null){ //检查是否有当前模式，若没有应取默认值
                                    Pattern pattern = new Pattern();
                                    System.out.println("No Broadcast info, Using default pattern "+pattern);
                                    map = new HashMap<>();
                                    map.put(0,new Pattern());
                                }

                                //得到当前用户的最近状态
                                String prevAction = prevActionState.value();
                                //若最近状态和当前状态符合模式则将结果加到返回集
                                if(map!=null && prevAction!=null){
                                    //0 id规则表示适用于所有userid，非0 id规则还需要patternId==userid
                                    if(!map.containsKey(action.getUserId()) && map.containsKey(0)){
                                        Pattern pattern = map.get(0);
                                        if(pattern.getFirstAction().equals(prevAction) && pattern.getSecondAction().equals(action.getAction())){
                                            out.collect(new Tuple2<>(action.getUserId(), pattern));
                                        }
                                    }else if(map.containsKey(action.getUserId())){
                                        Pattern pattern = map.get(action.getUserId());
                                        if(pattern.getFirstAction().equals(prevAction) && pattern.getSecondAction().equals(action.getAction())){
                                            out.collect(new Tuple2<>(action.getUserId(), pattern));
                                        }
                                    }
                                }
                                //更新最近状态为当前状态，以备下一次匹配
                                prevActionState.update(action.getAction());
                            }

                            /* 被广播流里的每个元素调用，替换最近模式为当前模式 */
                            @Override
                            public void processBroadcastElement(Map<Integer,Pattern> map, Context ctx, Collector<Tuple2<Integer, Pattern>> out) throws Exception {
                                System.out.println("Pattern Updated as: "+map);
                                BroadcastState<Void, Map<Integer,Pattern>> bcState = ctx.getBroadcastState(bcStateDescriptor);
                                bcState.put(null, map);
                            }
                        });
        matches.print();
        try {
            env.execute("Broadstate User Action");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Action{
        private Integer userId;
        private String action;

        public Action(){}

        public Action(Integer userId,String action){
            this.userId=userId;
            this.action=action;
        }

        public Integer getUserId() {
            return userId;
        }

        public void setUserId(Integer userId) {
            this.userId = userId;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern{
        private Integer patternId;
        private String firstAction;
        private String secondAction;

        public Pattern(){
            this.patternId=0;
            this.firstAction="a";
            this.secondAction="b";
        }

        public Pattern(Integer patternId,String firstAction,String secondAction){
            this.patternId=patternId;
            this.firstAction=firstAction;
            this.secondAction=secondAction;
        }

        public String getFirstAction() {
            return firstAction;
        }

        public void setFirstAction(String firstAction) {
            this.firstAction = firstAction;
        }

        public String getSecondAction() {
            return secondAction;
        }

        public void setSecondAction(String secondAction) {
            this.secondAction = secondAction;
        }

        public Integer getPatternId() {
            return patternId;
        }

        public void setPatternId(Integer patternId) {
            this.patternId = patternId;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "patternId='" + patternId + '\'' +
                    ", firstAction='" + firstAction + '\'' +
                    ", secondAction='" + secondAction + '\'' +
                    '}';
        }
    }
}

