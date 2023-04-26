package org.lyb.calcite;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;

import java.util.List;

public class CalciteTest {
    public static Frameworks.ConfigBuilder config() {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        return Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .traitDefs((List<RelTraitDef>) null)
                .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
    }

    @Test
    public void testSqlNode() throws SqlParseException {
        SqlParser parser = SqlParser.create("select c,d from source where a = '6'", SqlParser.Config.DEFAULT);
        SqlNode sqlNode = parser.parseStmt();
        System.out.println(sqlNode);
    }

    @Test
    public void testScan() {
        final FrameworkConfig config = config().build();
        final RelBuilder builder = RelBuilder.create(config);
        final RelNode node = builder
                .scan("EMP")
                .build();
        System.out.println(RelOptUtil.toString(node));
    }
}
