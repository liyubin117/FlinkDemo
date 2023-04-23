package cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class TemperatureAlertDemo {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 输入流
        DataStream<TemperatureEvent> inputEventStream = env.fromElements(new TemperatureEvent("xyz",22.0),
                new TemperatureEvent("xyz",20.1), new TemperatureEvent("xyz",21.1),
                new TemperatureEvent("xyz",22.2), new TemperatureEvent("xyz",22.1),
                new TemperatureEvent("xyz",22.3), new TemperatureEvent("xyz",22.1),
                new TemperatureEvent("xyz",22.4), new TemperatureEvent("xyz",22.7),
                new TemperatureEvent("xyz",27.0), new TemperatureEvent("xyz",30.0));

        // 定义模式，检查10秒钟内温度是否高于26度
        Pattern<TemperatureEvent,?> warningPattern = Pattern.<TemperatureEvent>begin("start")
                .subtype(TemperatureEvent.class)
                .where(new IterativeCondition<TemperatureEvent>() {
                    private static final long serialVersionUID = 1L;
                    public boolean filter(TemperatureEvent value, Context<TemperatureEvent> ctx) throws Exception {
                        if(value.getTemperature() >= 26.0){
                            return true;
                        }
                        return false;
                    }
                })
                .within(Time.seconds(10));

        //输入流 + 模式 = 模式流
        PatternStream<TemperatureEvent> patternStream = CEP.pattern(inputEventStream, warningPattern);

        //在模式流上select事件,符合条件的发生警告，即输出
        DataStream<Alert> result= patternStream.select(new PatternSelectFunction<TemperatureEvent, Alert>() {
                    private static final long serialVersionUID = 1L;
                    public Alert select(Map<String, List<TemperatureEvent>> event) throws Exception {
                        System.out.println(event.size());
                        TemperatureEvent e = event.get("start").get(0);
                        return new Alert("Temperature Rise Detected: " + e.getTemperature() + " on machine name: " + e.getMachineName());
                    }
                });

        result.print();

        env.execute("CEP on Temperature Sensor");
    }
}