package org.lyb.javacc;

public class CodeGenMain {

    public static void main(String[] args) throws Exception {
        //        javacc();
        adder();
    }

    private static void version() throws Exception {
        org.javacc.parser.Main.main(new String[] {"-version"});
    }

    private static void javacc() throws Exception {

        String path = CodeGenMain.class.getClassLoader().getResource("Simple1.jj").getPath();
        org.javacc.parser.Main.main(new String[] {path});
    }

    private static void adder() throws Exception {

        String path = CodeGenMain.class.getClassLoader().getResource("Adder.jj").getPath();
        org.javacc.parser.Main.main(new String[] {path});
    }
}
