package base;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;

public class ParameterToolTest {
    private static int str;

    static {
        str = 1000;
    }

    public static void main(String[] args) throws IOException {
        // 从命令行参数获取配置
        ParameterTool pt = ParameterTool.fromArgs(args);
        if (pt.getNumberOfParameters() < 2) {
            System.out.println(
                    "Missing parameters!\n"
                            + "Usage: Kafka --input-topic <topic> --output-topic <topic> ");
        }
        String confFile = pt.get("conf-file");
        System.out.println(confFile);
        System.out.println(pt.get("other"));

        // 从properties文件获取配置
        pt = ParameterTool.fromPropertiesFile(new File(confFile));
        System.out.println(pt.get("Key1"));
        System.out.println(pt.get("null").length());
        System.out.println(pt.get("key3"));
        System.out.println(pt.get("json"));

        HashMap<String, String> defaultValCols = new HashMap<>();
        String defaultValColsStr = pt.get("defaultValColsStr");
        JSONObject jo = JSONObject.parseObject(defaultValColsStr);
        for (String key : jo.keySet()) {
            defaultValCols.put(key, (String) jo.get(key));
        }
        System.out.println(defaultValCols);

        // 从环境变量获取配置
        pt = ParameterTool.fromSystemProperties();
        for (Map.Entry entry : pt.toMap().entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
        System.out.println(pt.get("confFile"));

        str = 2000;
        System.out.println(str);
        test();
        System.out.println(str);
    }

    private static void test() {
        System.out.println(str);
        str = 3000;
    }
}
