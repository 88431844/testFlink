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
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(uuid)){
            return 0L;
        }
        long count = 0L;
        String sql = "select count(1) from " + tableName +" where uuid = " + uuid;
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

    public static void main(String[] args) {
        String tableName = "account_wechat_match";
        System.out.println(getCount(tableName,"'a6f12af99d904484b37b67e3b3a1ce41'"));
    }

//    public static void exeSql(String sql){
//        String address = "jdbc:clickhouse://172.16.200.16:8123/bi_calculator";
//        Connection connection = null;
//        Statement statement = null;
//        ResultSet results = null;
//        try {
//            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
//            connection = DriverManager.getConnection(address);
//            statement = connection.createStatement();
//            long begin = System.currentTimeMillis();
//            results = statement.executeQuery(sql);
//            long end = System.currentTimeMillis();
//            System.out.println("执行（"+sql+"）耗时："+(end-begin)+"ms");
//            ResultSetMetaData rsmd = results.getMetaData();
//            List<Map> list = new ArrayList();
//            while(results.next()){
//                Map map = new HashMap();
//                for(int i = 1;i<=rsmd.getColumnCount();i++){
//                    map.put(rsmd.getColumnName(i),results.getString(rsmd.getColumnName(i)));
//                }
//                list.add(map);
//            }
//            for(Map map : list){
//                System.err.println(map);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }finally {//关闭连接
//            try {
//                if(results!=null){
//                    results.close();
//                }
//                if(statement!=null){
//                    statement.close();
//                }
//                if(connection!=null){
//                    connection.close();
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }
}
