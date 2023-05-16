package org.lyb.javacc;

import static org.junit.Assert.assertEquals;

import codegen.javacc.adder.Adder;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.Ignore;
import org.junit.Test;

public class JavaccTest {

    @Test
    public void testAdder() throws codegen.javacc.adder.ParseException {
        InputStream is = new ByteArrayInputStream("11.22 + 2 + 4".getBytes());
        Adder parser = new Adder(is);
        double result = parser.evaluate();
        assertEquals(17.22, result, 0);
    }

    @Test
    @Ignore
    public void testSimple1() throws codegen.javacc.adder.ParseException {
        InputStream is = new ByteArrayInputStream("{}".getBytes());
        Adder parser = new Adder(is);
        parser.evaluate();
    }
}
