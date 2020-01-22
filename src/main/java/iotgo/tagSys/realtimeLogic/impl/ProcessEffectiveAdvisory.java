package iotgo.tagSys.realtimeLogic.impl;

import iotgo.bean.MysqlBinlogInfo;
import iotgo.bean.UserTag;
import iotgo.tagSys.realtimeLogic.ProcessTagIF;
import iotgo.util.JedisUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import static iotgo.util.Const.*;
import static iotgo.util.Const.TAG_TYPE_USER_TOUCH;
import static iotgo.util.FilterUtil.getMysqlBinlogInfo;
import static iotgo.util.StringUtil.getUserTagRedis;
import static iotgo.util.StringUtil.isNotEmpty;

/**
 * 处理 有效咨询 标签 逻辑
 */
public class ProcessEffectiveAdvisory implements ProcessTagIF {
    @Override
    public SingleOutputStreamOperator<UserTag> process(SingleOutputStreamOperator<String> sso, StreamTableEnvironment tableEnvironment) {

        return getMysqlBinlogInfo(sso, DATABASE_USER_CENTER, TABLE_WECHAT_EVENT, null)
                .filter(this::filter)
                .map(m -> UserTag.builder()
                        .uuid(String.valueOf(m.getData().get("uuid")))
                        .tagName(TAG_NAME_EFFECTIVE_ADVISORY)
                        .tagType(TAG_TYPE_USER_TOUCH)
                        .haveTag(true)
                        .build());
    }


    @Override
    public boolean filter(MysqlBinlogInfo mysqlBinlogInfo) {
        //只接收添加和修改的数据库操作
        if (mysqlBinlogInfo.getType().equals(OP_TYPE_INSERT) || mysqlBinlogInfo.getType().equals(OP_TYPE_UPDATE)) {
            //确认必要key存在
            if (isNotEmpty(String.valueOf(mysqlBinlogInfo.getData().get("event")))
                    && isNotEmpty(String.valueOf(mysqlBinlogInfo.getData().get("content_type")))) {
                //判断该用户是否已经拥有该标签
                String redisKey = PROCESSED_TAG_REDIS_PREFIX + getUserTagRedis(String.valueOf(mysqlBinlogInfo.getData().get("uuid")), TAG_NAME_EFFECTIVE_ADVISORY);
                String haveTag = JedisUtil.getJedis().get(redisKey);
                //如果该用户没有处理过该标签，则按照逻辑判断是否拥有
                if (null == haveTag) {
                    return mysqlBinlogInfo.getData().get("event").equals("0")
                            && mysqlBinlogInfo.getData().get("content_type").equals("1");
                }
            }
        }
        return false;
    }

}
