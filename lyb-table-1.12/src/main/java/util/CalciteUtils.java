package util;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

public class CalciteUtils {
    public static SqlParser createFlinkParser(String expr) {
        SqlParser.Config parserConfig =
                SqlParser.configBuilder()
                        .setParserFactory(FlinkSqlParserImpl.FACTORY)
                        .setLex(Lex.JAVA)
                        .setIdentifierMaxLength(256)
                        .build();

        return SqlParser.create(expr, parserConfig);
    }

    public static Map<SqlIdentifier, SqlIdentifier> extractLineageFromCreateLike(SqlNode sqlNode) {
        final Map<SqlIdentifier, SqlIdentifier> result = new HashMap<>();
        if (sqlNode instanceof SqlCreateTable) {
            SqlCreateTable createTable = (SqlCreateTable) sqlNode;
            Optional<SqlTableLike> like = createTable.getTableLike();
            if (like.isPresent()) {
                SqlTableLike tableLike = like.get();
                result.put(createTable.getTableName(), tableLike.getSourceTable());
            }
        }
        return result;
    }
}
