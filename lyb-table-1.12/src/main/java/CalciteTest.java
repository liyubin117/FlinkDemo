import static org.junit.Assert.assertEquals;
import static util.CalciteUtils.createFlinkParser;
import static util.CalciteUtils.extractLineageFromCreateLike;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;

public class CalciteTest {
    @Test
    public void testExtractCreateLikeLineage() throws SqlParseException {
        SqlNode actualNode =
                createFlinkParser(
                                "CREATE TABLE a.b.c (\n"
                                        + "   a STRING\n"
                                        + ")\n"
                                        + "LIKE d.e.f (\n"
                                        + "   EXCLUDING PARTITIONS\n"
                                        + "   EXCLUDING CONSTRAINTS\n"
                                        + "   EXCLUDING WATERMARKS\n"
                                        + "   OVERWRITING GENERATED\n"
                                        + "   OVERWRITING OPTIONS\n"
                                        + ")")
                        .parseStmt();

        System.out.println(actualNode);
        extractLineageFromCreateLike(actualNode)
                .forEach(
                        (k, v) -> {
                            assertEquals("a.b.c", k.toString());
                            assertEquals("d.e.f", v.toString());
                        });
    }
}
