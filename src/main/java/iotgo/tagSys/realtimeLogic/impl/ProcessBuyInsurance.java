package iotgo.tagSys.realtimeLogic.impl;

import iotgo.bean.MysqlBinlogInfo;
import iotgo.bean.UserTag;
import iotgo.tagSys.realtimeLogic.ProcessTagIF;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import static iotgo.util.Const.*;
import static iotgo.util.Const.TAG_TYPE_USER_TOUCH;
import static iotgo.util.FilterUtil.getMysqlBinlogInfo;
import static iotgo.util.StringUtil.isNotEmpty;

/**
 * 处理 买保险服务 标签 逻辑
 */
public class ProcessBuyInsurance implements ProcessTagIF {

    @Override
    public SingleOutputStreamOperator<UserTag> process(SingleOutputStreamOperator<String> sso, StreamTableEnvironment tableEnvironment) {

        return getMysqlBinlogInfo(sso, DATABASE_PAYMENT, TABLE_PAYMENT_ORDER,null)
                .filter(this::filter)
                .map(m -> UserTag.builder()
                        .uuid(String.valueOf(m.getData().get("uuid")))
                        .tagName(TAG_NAME_BUY_INSURANCE)
                        .tagType(TAG_TYPE_USER_TOUCH)
                        .haveTag(true)
                        .build());
    }

    @Override
    public boolean filter(MysqlBinlogInfo mysqlBinlogInfo) {
        if (mysqlBinlogInfo.getType().equals(OP_TYPE_INSERT) || mysqlBinlogInfo.getType().equals(OP_TYPE_UPDATE)) {
            if (isNotEmpty(String.valueOf(mysqlBinlogInfo.getData().get("status")))
                    && isNotEmpty(String.valueOf(mysqlBinlogInfo.getData().get("app_goods_sn")))) {
                return mysqlBinlogInfo.getData().get("app_goods_sn").equals("ins1v1")
                        && mysqlBinlogInfo.getData().get("status").equals("1");
            }
        }
        return false;
    }
}
