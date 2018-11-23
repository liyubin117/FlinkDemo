package Table;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

public class JavaSetTable{
    public static void main(String[] args) throws Exception {
        //配置环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        //指定输入
        DataSet<String> input = env.fromElements(
                "1,liyubin,26",
                "2,li,27",
                "3,yu,25",
                "4,bin,26",
                "4,bin,26"
        );
        //生成DataSet
        DataSet<Tuple3<Integer,String,Integer>> personSet = input.map(new MapFunction<String, Tuple3<Integer,String,Integer>>() {
            public Tuple3<Integer,String,Integer> map(String value) throws Exception {
                String[] x = value.split(",");
                return new Tuple3(Integer.valueOf(x[0]), x[1], Integer.valueOf(x[2]));
            }
        });
        //指定表程序
//        Table counts = tableEnv.fromDataSet(personSet,"id,name,age").filter("age>=26").groupBy("age").select("age,id.count() as cnt");

        tableEnv.registerDataSet("person", personSet,"id, name, age");
        Table counts = tableEnv.sqlQuery("select age,count(1) from person where age>=26 group by age");


//        //结果转化为DataSet
        DataSet<Row> result = tableEnv.toDataSet(counts,Row.class);
        result.print();
}


}

