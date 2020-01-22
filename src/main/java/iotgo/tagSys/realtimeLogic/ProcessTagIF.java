package iotgo.tagSys.realtimeLogic;

import iotgo.bean.MysqlBinlogInfo;
import iotgo.bean.UserTag;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 处理 标签逻辑 接口
 */
public interface ProcessTagIF {

    SingleOutputStreamOperator<UserTag> process(SingleOutputStreamOperator<String> sso, StreamTableEnvironment tableEnvironment);

    boolean filter(MysqlBinlogInfo mysqlBinlogInfo);
}
