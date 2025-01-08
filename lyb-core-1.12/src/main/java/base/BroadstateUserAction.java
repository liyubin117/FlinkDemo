package base;

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

/** nc -lk my 9888 100,a 200,b 100,b 200,a nc -lk my 9889 a,b */
public class BroadstateUserAction {
    /* 创建 Broadcast State 的描述 MapStateDescriptor */
    private static final MapStateDescriptor<Void, Pattern> bcStateDescriptor =
            new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Action> actions =
                env.socketTextStream("localhost", 9888)
                        .filter((String str) -> str.split(",").length == 2)
                        .map(
                                (String str) ->
                                        new Action(
                                                Long.valueOf(str.split(",")[0]),
                                                str.split(",")[1]));
        DataStream<Pattern> patterns =
                env.socketTextStream("localhost", 9889)
                        .filter((String str) -> str.split(",").length == 2)
                        .map((String str) -> new Pattern(str.split(",")[0], str.split(",")[1]));
        /* 设置Key的字段，同样用户的数据分到相同的operator里 */
        KeyedStream<Action, Long> actionsByUser =
                actions.keyBy((KeySelector<Action, Long>) action -> action.getUserId());

        /* 定制 Broadcast Stream */
        BroadcastStream<Pattern> bcedPatterns = patterns.broadcast(bcStateDescriptor);
        /* 连接两个流并应用规则 */
        DataStream<Tuple2<Long, Pattern>> matches =
                actionsByUser
                        .connect(bcedPatterns)
                        .process(
                                // 参数依次是键、主数据流（Action）、广播流（Pattern）、返回结果
                                new KeyedBroadcastProcessFunction<
                                        Object, Action, Pattern, Tuple2<Long, Pattern>>() {
                                    // 当前用户的最近状态
                                    ValueState<String> prevActionState;

                                    @Override
                                    public void open(Configuration conf) {
                                        // 初始化最近状态
                                        prevActionState =
                                                getRuntimeContext()
                                                        .getState(
                                                                new ValueStateDescriptor<String>(
                                                                        "lastAction",
                                                                        Types.STRING));
                                    }

                                    /* 被主数据流里的每个元素调用，最近状态与当前状态结合起来，检查是否符合广播流里的模式 */
                                    @Override
                                    public void processElement(
                                            Action action,
                                            ReadOnlyContext ctx,
                                            Collector<Tuple2<Long, Pattern>> out)
                                            throws Exception {
                                        // 得到当前模式
                                        Pattern pattern =
                                                ctx.getBroadcastState(bcStateDescriptor).get(null);
                                        if (pattern == null) { // 检查是否有当前模式，若没有应取默认值
                                            System.out.println(
                                                    "No Broadcast info, Using default pattern a---b !!!");
                                            pattern = new Pattern();
                                        }
                                        // 得到当前用户的最近状态
                                        String prevAction = prevActionState.value();
                                        // 若最近状态和当前状态符合模式则将结果加到返回集
                                        if (pattern != null && prevAction != null) {
                                            if (pattern.getFirstAction().equals(prevAction)
                                                    && pattern.getSecondAction()
                                                            .equals(action.getAction())) {
                                                out.collect(
                                                        new Tuple2<>(action.getUserId(), pattern));
                                            }
                                        }
                                        // 更新最近状态为当前状态，以备下一次匹配
                                        prevActionState.update(action.getAction());
                                    }

                                    /* 被广播流里的每个元素调用，替换最近模式为当前模式 */
                                    @Override
                                    public void processBroadcastElement(
                                            Pattern pattern,
                                            Context ctx,
                                            Collector<Tuple2<Long, Pattern>> out)
                                            throws Exception {
                                        System.out.println(
                                                "Pattern Updated as: "
                                                        + pattern.getFirstAction()
                                                        + "---"
                                                        + pattern.getSecondAction());
                                        BroadcastState<Void, Pattern> bcState =
                                                ctx.getBroadcastState(bcStateDescriptor);
                                        bcState.put(null, pattern);
                                    }
                                });
        matches.print();
        try {
            env.execute("Broadstate User Action");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Action {
        private Long userId;
        private String action;

        public Action() {}

        public Action(Long userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }
    }

    public static class Pattern {
        private String firstAction;
        private String secondAction;

        public Pattern() {
            this.firstAction = "a";
            this.secondAction = "b";
        }

        public Pattern(String firstAction, String secondAction) {
            this.firstAction = firstAction;
            this.secondAction = secondAction;
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

        @Override
        public String toString() {
            return "matches:" + this.firstAction + "---" + this.secondAction;
        }
    }
}
