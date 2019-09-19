package iotgo.tagSys.realtimeLogic;

import iotgo.bean.MysqlBinlogInfo;
import iotgo.bean.UserTag;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import static iotgo.util.Const.*;
import static iotgo.util.FilterUtil.getMysqlBinlogInfo;

/**
 * 处理 添加好友 标签 逻辑
 */
public class ProcessAddFriend implements ProcessTagIF{
    @Override
    public SingleOutputStreamOperator<UserTag> process(SingleOutputStreamOperator<String> sso) {

        return getMysqlBinlogInfo(sso,DATABASE_USER_CENTER, TABLE_ACCOUNT_WECHAT_MATCH, OP_TYPE_INSERT)
                .map(m -> UserTag.builder()
                        .uuid(String.valueOf(m.getData().get("uuid")))
                        .tagName(TAG_NAME_ADD_FRIEND)
                        .tagType(TAG_TYPE_USER_TOUCH)
                        .haveTag(true)
                        .build());
    }

    @Override
    public boolean filter(MysqlBinlogInfo mysqlBinlogInfo) {
        return false;
    }
}
