package org.lyb.calcite;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class Utils {
    /** 根据SqlNode提取源表 */
    public static List<String> extractTableNameList(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql);
        SqlNode parsed = parser.parseQuery();
        List<String> tableNameList = new ArrayList<>();

        parseSqlNode(parsed, tableNameList);

        return tableNameList;
    }

    private static void parseFromNode(SqlNode from, List<String> tableNameList) {
        SqlKind kind = from.getKind();
        switch (kind) {
            case IDENTIFIER:
                // 最终的表名
                SqlIdentifier sqlIdentifier = (SqlIdentifier) from;
                tableNameList.add(sqlIdentifier.toString());
                break;
            case AS:
                SqlBasicCall sqlBasicCall = (SqlBasicCall) from;
                SqlNode selectNode = sqlBasicCall.getOperandList().get(0);
                parseSqlNode(selectNode, tableNameList);
                break;
            case JOIN:
                SqlJoin sqlJoin = (SqlJoin) from;
                SqlNode left = sqlJoin.getLeft();
                parseFromNode(left, tableNameList);
                SqlNode right = sqlJoin.getRight();
                parseFromNode(right, tableNameList);
                break;
            case SELECT:
                parseSqlNode(from, tableNameList);
                break;
        }
    }

    private static void parseSqlNode(SqlNode sqlNode, List<String> tableNameList) {
        SqlKind kind = sqlNode.getKind();
        switch (kind) {
            case IDENTIFIER:
                parseFromNode(sqlNode, tableNameList);
                break;
            case SELECT:
                SqlSelect select = (SqlSelect) sqlNode;
                parseFromNode(select.getFrom(), tableNameList);
                break;
            case UNION:
                ((SqlBasicCall) sqlNode)
                        .getOperandList()
                        .forEach(
                                node -> {
                                    parseSqlNode(node, tableNameList);
                                });

                break;
            case ORDER_BY:
                handlerOrderBy(sqlNode, tableNameList);
                break;
        }
    }

    private static void handlerOrderBy(SqlNode node, List<String> tableNameList) {
        SqlOrderBy sqlOrderBy = (SqlOrderBy) node;
        SqlNode query = sqlOrderBy.query;
        parseSqlNode(query, tableNameList);
    }
}
