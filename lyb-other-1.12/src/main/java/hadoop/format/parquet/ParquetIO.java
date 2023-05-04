package hadoop.format.parquet;

import static hadoop.format.parquet.Utils.DELIMITER;
import static hadoop.format.parquet.Utils.parquetMetadataRead;
import static hadoop.format.parquet.Utils.parquetWrite;

import com.github.javafaker.Book;
import com.github.javafaker.Faker;
import java.text.SimpleDateFormat;
import java.util.Random;

/** @author Yubin Li */
public class ParquetIO {

    public static void main(String[] args) throws Exception {
        Faker faker = new Faker();
        Random random = new Random();
        StringBuffer buffer = new StringBuffer();
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (long i = 0; i < 199; i++) {
            Book book = faker.book();
            String desc =
                    book.title()
                            + DELIMITER
                            + Math.random() * 100
                            + DELIMITER
                            + random.nextInt(1000)
                            + DELIMITER
                            + book.author()
                            + DELIMITER
                            + parser.format(faker.date().birthday());
            buffer.append(desc + "\n");
        }
        String content = buffer.toString();
        System.out.println(content);

        parquetWrite("file/out.file", content);
        //        parquetRead("file/out.file");
        parquetMetadataRead("file/out.file");
    }
}
