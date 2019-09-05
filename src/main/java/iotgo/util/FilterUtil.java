package iotgo.util;

import com.alibaba.fastjson.JSON;
import iotgo.bean.MysqlBinlogInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 过滤工具类
 */
public class FilterUtil {
//
//    /**
//     * 校验 数据库为 user_center,表为account_wechat_match
//     * @param json
//     * @return
//     */
//    public static boolean filterADD_FRIEND(String json){
//        MysqlBinlogInfo mysqlBinlogInfo = JSON.parseObject(json,MysqlBinlogInfo.class);
//        if (Const.DATABASE_USER_CENTER.equals(mysqlBinlogInfo.getDatabase())){
//            return Const.TABLE_ACCOUNT_WECHAT_MATCH.equals(mysqlBinlogInfo.getTable());
//        }
//        return false;
//
//    }
//
//    public static boolean filterDatabase(String json,String filterDatabase){
//        MysqlBinlogInfo mysqlBinlogInfo = JSON.parseObject(json,MysqlBinlogInfo.class);
//        return filterDatabase.equals(mysqlBinlogInfo.getDatabase());
//    }

    /**
     * 以数据库 表 操作类型纬度 过滤 MySql binlog
     * @param mysqlBinlog
     * @param database
     * @param table
     * @param type
     * @return
     */
    public static SingleOutputStreamOperator<MysqlBinlogInfo> filterMySqlBinlog(SingleOutputStreamOperator<String> mysqlBinlog,String database, String table, String type){
        return mysqlBinlog
                .map(m -> JSON.parseObject(m,MysqlBinlogInfo.class))
                .filter(f -> database.equals(f.getDatabase()) && table.equals(f.getTable()) && type.equals(f.getType()));
    }

}
