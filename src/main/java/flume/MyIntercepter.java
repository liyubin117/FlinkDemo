package flume;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义插件用于指定host和app
 * a1.sources.r1.interceptors = i1
 * a1.sources.r1.interceptors.i1.type = me.jinkun.flume.MyIntercepter$Builder
 * a1.sources.r1.interceptors.i1.host=server1
 * a1.sources.r1.interceptors.i1.app=nginx
 * a1.sources.r1.interceptors.i1.fileName=access.log
 * a1.sources.r1.interceptors.i1.delimiter=|
 *
 */
public class MyIntercepter implements Interceptor {
    private String host;
    private String app;
    private String fileName;
    private String delimiter;

    public MyIntercepter(String host, String app, String fileName, String delimiter) {
        this.host = host;
        this.app = app;
        this.fileName = fileName;
        this.delimiter = delimiter;
    }

    public void initialize() {
        System.out.println("我的拦截器已经启动");
    }

    public Event intercept(Event event) {
        if (event == null) {
            return null;
        }
        try {
            String line = new String(event.getBody(), Charsets.UTF_8);
            StringBuilder sb = new StringBuilder();
            sb.append(host);
            sb.append(delimiter);
            sb.append(app);
            sb.append(delimiter);
            sb.append(fileName);
            sb.append(delimiter);
            sb.append(line);
            event.setBody(sb.toString().getBytes(Charsets.UTF_8));
            return event;
        } catch (Exception e) {
            return event;
        }
    }

    public List<Event> intercept(List<Event> list) {
        List<Event> out = new ArrayList<Event>(list.size());
        for (Event event : list) {
            Event outEvent = intercept(event);
            if (outEvent != null) {
                out.add(outEvent);
            }
        }
        return out;
    }

    public void close() {
        System.out.println("我的拦截器已经关闭");
    }

    /**
     *
     */
    public static class Constants {
        public static final String HOST = "host";
        public static final String HOST_DEFAULT = "UNKNOW";
        public static final String APP = "app";
        public static final String APP_DEFAULT = "UNKNOW";
        public static final String FILE_NAME = "fileName";
        public static final String FILE_NAME_DEFAULT = "UNKNOW";
        public static final String DELIMITER = "delimiter";
        public static final String DELIMITER_DEFAULT = "|";
    }

    /**
     * 获取配置文件
     */
    public static class Builder implements Interceptor.Builder {

        private String host;
        private String app;
        private String fileName;
        private String delimiter;

        /*
         * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
         */
        public void configure(Context context) {
            host = context.getString(Constants.HOST, Constants.HOST_DEFAULT);
            app = context.getString(Constants.APP, Constants.APP_DEFAULT);
            fileName = context.getString(Constants.FILE_NAME, Constants.FILE_NAME_DEFAULT);
            delimiter = context.getString(Constants.DELIMITER, Constants.DELIMITER_DEFAULT);
        }

        /*
         * @see org.apache.flume.interceptor.Interceptor.Builder#build()
         */
        public Interceptor build() {
            return new MyIntercepter(host, app, fileName, delimiter);
        }
    }
}