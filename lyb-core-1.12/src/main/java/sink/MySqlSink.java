package sink;

import java.sql.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务端 create table device(device_id varchar(200) primary key,device_no int,name varchar(200),gender
 * varchar(100));
 */
public abstract class MySqlSink<T> extends RichSinkFunction<T> {
    private final Logger log = LoggerFactory.getLogger(MySqlSink.class);
    private static final long serialVersionUID = 1L;

    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;

    private String driver;
    private String url;
    private String user;
    private String pwd;
    private String sql;

    public MySqlSink(String driver, String url, String user, String pwd, String sql) {
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.pwd = pwd;
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = JDBCUtil.getConnection(driver, url, user, pwd);
        preparedStatement = connection.prepareStatement(sql);
        log.info("Flink-mysql open connection");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        JDBCUtil.closeResource(resultSet, preparedStatement);
        super.close();
        log.info("Flink-mysql close connection");
    }

    public PreparedStatement getPreparedStatement() {
        return this.preparedStatement;
    }

    public Logger getLogger() {
        return this.log;
    }

    private static class JDBCUtil {
        private static Connection conn;

        public static Connection getConnection(String driver, String url, String user, String pwd) {
            try {
                Class.forName(driver);
                conn = DriverManager.getConnection(url, user, pwd);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return conn;
        }

        public static void closeResource(ResultSet resultSet, PreparedStatement preparedStatement) {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            // 等待垃圾回收
            resultSet = null;

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            // 等待垃圾回收
            preparedStatement = null;

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            // 等待垃圾回收
            conn = null;
        }
    }

    //    public static class Record{
    //        private String device_id;
    //        private Integer device_no;
    //        private String name;
    //        private String gender;
    //        public Record(String device_id,Integer device_no,String name,String gender){
    //            this.device_id=device_id;
    //            this.device_no=device_no;
    //            this.name=name;
    //            this.gender=gender;
    //        }
    //    }

}
