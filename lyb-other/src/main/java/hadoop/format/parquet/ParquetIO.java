package hadoop.format.parquet;

import com.github.javafaker.Book;
import com.github.javafaker.Faker;

import java.util.Random;

import static hadoop.format.parquet.Utils.*;

/**
 * @author Yubin Li
 */
public class ParquetIO {

    public static void main(String[] args) throws Exception {
        Faker faker = new Faker();
        Random random = new Random();
        StringBuffer buffer = new StringBuffer();
        for (long i = 0; i < 199; i++) {
            Book book = faker.book();
            String desc = book.title() + DELIMITER + Math.random() * 100 + DELIMITER + random.nextInt(1000) + DELIMITER + book.author();
//            System.out.println(desc);
            buffer.append(desc + "\n");
        }
        String content = buffer.toString();

        parquetWrite("file/out.file", content);
//        parquetRead("file/out.file");
        parquetMetadataRead("file/out.file");

    }
}