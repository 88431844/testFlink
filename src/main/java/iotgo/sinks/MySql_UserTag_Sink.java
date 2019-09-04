package iotgo.sinks;

import iotgo.bean.UserTag;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ResourceBundle;

public class MySql_UserTag_Sink extends RichSinkFunction<UserTag> {
    private Connection connection = null;
    private PreparedStatement ps = null;

    /**
     * 一、open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ResourceBundle rb = ResourceBundle.getBundle("mysql");
        String driver = rb.getString("driver");
        String url = rb.getString("url");
        String username = rb.getString("username");
        String password = rb.getString("password");
        //1.加载驱动
        Class.forName(driver);
        //2.创建连接
        connection = DriverManager.getConnection(url, username, password);
        String sql = "insert into user_tag(uuid,tag_name,tag_desc,tag_type,create_at,update_at) values(?,?,?,?,now(),now()) ON DUPLICATE KEY UPDATE update_at = now() ;";
        //3.获得执行语句
        ps = connection.prepareStatement(sql);
    }


    /**
     * 二、每个元素的插入都要调用一次invoke()方法，这里主要进行插入操作
     */
    @Override
    public void invoke(UserTag userTag) throws Exception {
        try {
            //4.组装数据，执行插入操作
            ps.setString(1, userTag.getUuid());
            ps.setString(2, userTag.getTagName());
            ps.setString(3, userTag.getTagDesc());
            ps.setString(4, userTag.getTagType());
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //5.关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

}