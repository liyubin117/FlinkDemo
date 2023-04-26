package org.lyb.javacc;



public class JavaccCodeGenMain {

    public static void main(String[] args) throws Exception {
       javacc();
    }

    private static void version() throws Exception {
        org.javacc.parser.Main.main(new String[] {"-version"});
    }

    private static void javacc() throws Exception {

        String path = JavaccCodeGenMain.class.getClassLoader().getResource("Simple1.jj").getPath();

        org.javacc.parser.Main.main(new String[] {path});
    }

}
