package iotgo.util;

import com.alibaba.fastjson.JSON;
import iotgo.bean.MysqlBinlogInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import static iotgo.util.Const.OP_TYPE_INSERT;
import static iotgo.util.Const.OP_TYPE_UPDATE;
import static iotgo.util.StringUtil.isNotEmpty;

/**
 * 过滤工具类
 */
public class FilterUtil {

    /**
     * 以数据库 表 操作类型维度 过滤 MySql binlog
     *
     * @param mysqlBinlog
     * @param database
     * @param table
     * @param type
     * @return
     */
    public static SingleOutputStreamOperator<MysqlBinlogInfo> filterMySqlBinlog(SingleOutputStreamOperator<String> mysqlBinlog, String database, String table, String type) {
        return mysqlBinlog
                .map(m -> JSON.parseObject(m, MysqlBinlogInfo.class))
                .filter(f -> database.equals(f.getDatabase()) && table.equals(f.getTable()) && type.equals(f.getType()));
    }

    /**
     * 以数据库 表 维度 过滤 MySql binlog
     *
     * @param mysqlBinlog
     * @param database
     * @param table
     * @return
     */
    public static SingleOutputStreamOperator<MysqlBinlogInfo> filterMySqlBinlogSub(SingleOutputStreamOperator<String> mysqlBinlog, String database, String table) {
        return mysqlBinlog
                .map(m -> JSON.parseObject(m, MysqlBinlogInfo.class))
                .filter(f -> database.equals(f.getDatabase()) && table.equals(f.getTable()));
    }

    /**
     * 买保险服务 过滤条件
     * @param mysqlBinlogInfo
     * @return
     */
    public static boolean filterBuyInsurance(MysqlBinlogInfo mysqlBinlogInfo) {
        if (mysqlBinlogInfo.getType().equals(OP_TYPE_INSERT) || mysqlBinlogInfo.getType().equals(OP_TYPE_UPDATE)) {
            if (isNotEmpty(String.valueOf(mysqlBinlogInfo.getData().get("trade_info")))
                    && isNotEmpty(String.valueOf(mysqlBinlogInfo.getData().get("status")))
                    && isNotEmpty(String.valueOf(mysqlBinlogInfo.getData().get("app_goods_sn")))) {
                return mysqlBinlogInfo.getData().get("app_goods_sn").equals("ins1v1")
                        && mysqlBinlogInfo.getData().get("status").equals("1");
            }
        }
        return false;
    }

}
