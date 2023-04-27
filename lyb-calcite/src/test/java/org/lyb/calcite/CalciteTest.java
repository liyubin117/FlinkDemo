package org.lyb.calcite;

import java.sql.*;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;

public class CalciteTest {
    public static Frameworks.ConfigBuilder config() {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add(
                "myTable",
                new AbstractTable() { // note: add a table
                    @Override
                    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                        RelDataTypeFactory.Builder builder = typeFactory.builder();

                        builder.add(
                                "column1",
                                new BasicSqlType(
                                        new RelDataTypeSystemImpl() {
                                        }, SqlTypeName.INTEGER));
                        builder.add(
                                "column2",
                                new BasicSqlType(new RelDataTypeSystemImpl() {
                                }, SqlTypeName.CHAR));
                        builder.add(
                                "AGE",
                                new BasicSqlType(
                                        new RelDataTypeSystemImpl() {
                                        }, SqlTypeName.INTEGER));
                        return builder.build();
                    }
                });
        return Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(SqlParser.Config.DEFAULT)
                .traitDefs((List<RelTraitDef>) null)
                .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
    }

    @Test
    public void testSqlNode() throws SqlParseException {
        SqlParser parser =
                SqlParser.create("select c,d from source where a = '6'", SqlParser.Config.DEFAULT);
        SqlNode sqlNode = parser.parseStmt();
        System.out.println(sqlNode);
    }

    @Test
    public void testScanRelNode() {
        final FrameworkConfig config = config().build();
        final RelBuilder builder = RelBuilder.create(config);
        RelNode query =
                builder.scan("myTable") // 扫描名为 myTable 的表
                        .filter(
                                builder.call(
                                        SqlStdOperatorTable.EQUALS, // 使用等于运算符
                                        builder.field("column1"), // 第一个操作数是列 column1
                                        builder.literal("value1") // 第二个操作数是字面量 value1
                                )) // 过滤出 column1 = 'value1' 的行
                        .project(
                                builder.field("column1"), // 选择列 column1
                                builder.field("column2") // 选择列 column2
                        ) // 只保留 column1 和 column2 两列
                        .sort(builder.field("column1")) // 按 column1 列升序排序
                        .build(); // 构建查询
        System.out.println(RelOptUtil.toString(query));
    }

    @Test
    public void testReadCustomTable() {
        try {

            String model =
                    "{\n" +
                            "    \"version\":\"1.0\",\n" +
                            "    \"defaultSchema\":\"TEST\",\n" +
                            "    \"schemas\":[\n" +
                            "        {\n" +
                            "            \"name\":\"TEST\",\n" +
                            "            \"type\":\"custom\",\n" +
                            "            \"factory\":\"org.lyb.calcite.CustomSchemaFactory\",\n" +
                            "            \"operand\":{\n" +
                            "\n" +
                            "            }\n" +
                            "        }\n" +
                            "    ]\n" +
                            "}";
            Connection connection =
                    DriverManager.getConnection("jdbc:calcite:model=inline:" + model);

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from test01");
            while (resultSet.next()) {
                System.out.println("data => ");
                System.out.println(resultSet.getObject("value"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
