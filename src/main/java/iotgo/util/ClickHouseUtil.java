package iotgo.util;

import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;

public class ClickHouseUtil {

    public static Connection getClickHouseConn() {
        Connection connection = null;
        ResourceBundle rb = ResourceBundle.getBundle("clickhouse");
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            connection = DriverManager.getConnection(rb.getString("url"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void close(Connection connection, ResultSet results, Statement statement) {
        try {
            if (results != null) {
                results.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static long getCount(String tableName,String uuid){
        return getCountByCondition(tableName,"uuid = '" + uuid +"'");
    }

    public static void main(String[] args) {
        String tableName = "account_wechat_match";
        System.out.println(getCount(tableName,"'a6f12af99d904484b37b67e3b3a1ce41'"));
    }

    public static long getCountByCondition(String tableName,String where) {
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(where)){
            return 0L;
        }
        long count = 0L;
        String sql = "select count(1) from " + tableName +" where " + where;
        Connection connection = getClickHouseConn();
        ResultSet resultSet = null;
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            resultSet = ps.executeQuery();
            while (resultSet.next()){
                count = Long.parseLong(resultSet.getString(1));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            close(connection,resultSet,ps);
        }
        return count;
    }
}
