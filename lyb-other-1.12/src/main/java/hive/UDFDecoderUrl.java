package hive;

import java.net.URLDecoder;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
        name = "decoder_url",
        value =
                "_FUNC_(url [,code][,count]) - decoder a URL from a String for count times using code as encoding scheme ",
        extended =
                ""
                        + "if count is not given ,the url will be decoderd for 2 time,"
                        + "if code is not given ,GBK is used")
public class UDFDecoderUrl extends UDF {
    private String url = null;
    private int times = 2;
    private String code = "GBK";

    public UDFDecoderUrl() {}

    public String evaluate(String urlStr, String srcCode, int count) {
        if (urlStr == null) {
            return null;
        }
        if (count <= 0) {
            return urlStr;
        }
        if (srcCode != null) {
            code = srcCode;
        }
        url = urlStr;
        times = count;
        for (int i = 0; i < times; i++) {
            url = decoder(url, code);
        }
        return url;
    }

    public String evaluate(String urlStr, String srcCode) {
        if (urlStr == null) {
            return null;
        }
        url = urlStr;
        code = srcCode;
        return evaluate(url, code, times);
    }

    public String evaluate(String urlStr, int count) {
        if (urlStr == null) {
            return null;
        }
        if (count <= 0) {
            return urlStr;
        }
        url = urlStr;
        times = count;

        return evaluate(url, code, times);
    }

    public String evaluate(String urlStr) {
        if (urlStr == null) {
            return null;
        }
        url = urlStr;
        return evaluate(url, code, times);
    }

    private String decoder(String urlStr, String code) {
        if (urlStr == null || code == null) {
            return null;
        }
        try {
            urlStr = URLDecoder.decode(urlStr, code);
        } catch (Exception e) {
            return null;
        }
        return urlStr;
    }
}
