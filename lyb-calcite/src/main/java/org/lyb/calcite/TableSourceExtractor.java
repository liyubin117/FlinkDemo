package org.lyb.calcite;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;

/** @author Yubin Li 继承RelVisitor，可用于遍历RelNode树，解析其中的源表并以列表形式返回 */
public class TableSourceExtractor extends RelVisitor {
    private List<RelOptTable> tables = new ArrayList<>();

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
            TableScan tableScan = (TableScan) node;
            RelOptTable table = tableScan.getTable();
            tables.add(table);
        }
        super.visit(node, ordinal, parent);
    }

    public List<RelOptTable> getTables() {
        return tables;
    }
}
