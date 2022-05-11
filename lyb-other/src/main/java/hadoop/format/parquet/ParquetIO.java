package hadoop.format.parquet;

import static hadoop.format.parquet.Utils.*;

/**
 * @author Yubin Li
 */
public class ParquetIO {

    public static void main(String[] args) throws Exception {
        String content = "1984 1.2\n" +
                "animal 2.3";

        parquetWrite("file/out.file", content);
        parquetRead("file/out.file");
        parquetMetadataRead("file/out.file");

    }
}