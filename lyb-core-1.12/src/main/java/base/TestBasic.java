package base;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestBasic {
    public static void main(String[] args) {
        String str = "0#2#0#5300556";
        System.out.println(str.lastIndexOf("#"));
        System.out.println(str.substring(0, str.lastIndexOf("#")));
        System.out.println(str.substring(str.lastIndexOf("#") + 1, str.length()));

        List list1 = Arrays.asList(str);
        list1.set(0, str.substring(0, str.lastIndexOf("#")));
        System.out.println(list1);

        String game = "ball";
        System.out.println(String.format("^%s_format_.*$", game));

        // Pattern
        Pattern p = Pattern.compile("a*b");
        String[] ss = p.split("aaa123bbb");
        Matcher m = p.matcher("aaaaab");
        boolean b = m.matches();
        String s = m.replaceAll("_");
        for (String i : ss) {
            System.out.println(i);
        }
        System.out.println(b);
        System.out.println(s);

        Pattern pattern = Pattern.compile("\\w+");
        Matcher matcher = pattern.matcher("hello abc bbc cbc ccc");
        System.out.println(matcher.groupCount());
        //        System.out.println(matcher.group(1));
        System.out.println(matcher.matches());
        // find向前迭代
        while (matcher.find()) {
            System.out.println(matcher.group());
        }

        Pattern pattern2 = Pattern.compile("(\\w+)\\d+");
        Matcher matcher2 = pattern.matcher("hello123 abc bbc cbc ccc");
        matcher2.find();
        System.out.println(matcher2.groupCount());
    }
}
