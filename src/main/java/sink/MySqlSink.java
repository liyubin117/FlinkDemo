package sink;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;

/**
 * 服务端
 * create table device(device_id varchar(200) primary key,device_no int,name varchar(200),gender varchar(100));
 */
public class MySqlSink extends RichSinkFunction<Tuple4<String,Integer,String,String>> {
    private Logger log = LoggerFactory.getLogger(MySqlSink.class);
    private static final long serialVersionUID = 1L;

    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;


    private String driver;
    private String url;
    private String user;
    private String pwd;
    private String sql;
    public MySqlSink(String driver,String url,String user,String pwd,String sql){
        this.driver=driver;
        this.url=url;
        this.user=user;
        this.pwd=pwd;
        this.sql=sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = JDBCUtil.getConnection(driver,url,user,pwd);
        preparedStatement = connection.prepareStatement(sql);
        log.info("open connection");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        JDBCUtil.closeResource(resultSet,preparedStatement);
        super.close();
        log.info("close connection");
    }

    @Override
    public void invoke(Tuple4<String,Integer,String,String> value) throws Exception {
        if(value.f0 != "!!!error input!!!"){
            preparedStatement.setString(1, value.f0);
            preparedStatement.setInt(2, value.f1);
            preparedStatement.setString(3, value.f2);
            preparedStatement.setString(4, value.f3);
            preparedStatement.executeUpdate();
            log.info("update key:   "+value.f0);
        }
    }

    public static class JDBCUtil{
        private static Connection conn;


        public static Connection getConnection(String driver,String url,String user,String pwd){
            try {
                Class.forName(driver);
                conn = DriverManager.getConnection(url,user,pwd);
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
            return conn;
        }

        public static void closeResource(ResultSet resultSet,PreparedStatement preparedStatement){
            if(resultSet !=null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            //等待垃圾回收
            resultSet = null;

            if(preparedStatement !=null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            //等待垃圾回收
            preparedStatement = null;

            if(conn !=null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            //等待垃圾回收
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
