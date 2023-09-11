/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.concurrent.ExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class Test1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
//        tableEnvironment.executeSql(
//                "create table t2(id int comment 'hello') comment 'c123 ''c12''' with ('connector'='print')");
//        tableEnvironment.executeSql("insert into t2 select 1 as id");
//        TableResult result = tableEnvironment.executeSql("desc t2");
//        for (CloseableIterator<Row> it = result.collect(); it.hasNext(); ) {
//            Row row = it.next();
//            System.out.println(row);
//        }
//        tableEnvironment.executeSql("show create table t2").print();
        tableEnvironment.executeSql("CREATE TABLE kafka (\n"
                + "  `id` int,\n"
                + "  `name` string,\n"
                + "  `time1` time(9)\n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = 'test',\n"
                + "  'properties.bootstrap.servers' = 'datassert-kafka1.dg.163.org:9092,datassert-kafka2.dg.163.org:9092',\n"
                + "  'properties.group.id' = 'testGroup2',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n"
                + "  'format' = 'json'\n"
                + ")");
        tableEnvironment.executeSql("create table t1 (id int,name string, time1 time(9)) with ('connector' = 'print')");
        tableEnvironment.executeSql("create table t2 (id int,name string, time1 time(9)) with ('connector' = 'datagen', 'number-of-rows' = '2')");
        tableEnvironment.executeSql("insert into t1 select * from t2");
        tableEnvironment.executeSql("insert into kafka select * from t2");
        tableEnvironment.executeSql("insert into kafka select 1 as id, 'a' as name, TIME '12:12:12.123456' as time1");
//        tableEnvironment.executeSql("show columns in orders3 like 'p%'").print();
//        tableEnvironment.from("orders3").printSchema();
//        tableEnvironment.executeSql("show create table orders3").print();
    }
}
