package iotgo.tagSys.offlineLogic;

import iotgo.bean.UserTag;
import iotgo.util.ClickHouseUtil;
import lombok.extern.slf4j.Slf4j;
import org.mortbay.util.ajax.JSON;

import static iotgo.util.Const.*;

/**
 * 用户触达系统 标签处理逻辑
 */
@Slf4j
public class UserTouchLogic {

    /**
     * 离线逻辑 添加好友
     * @param uuid
     * @return
     */
    public static UserTag addFriend(String uuid) {
        long count = ClickHouseUtil.getCount(TABLE_ACCOUNT_WECHAT_MATCH, uuid);
        log.info("addFriend clickhouse count:" + count);
        return UserTag.builder().uuid(uuid).tagName(TAG_NAME_ADD_FRIEND).tagType(TAG_TYPE_USER_TOUCH).haveTag(haveTag(count)).build();
    }

    /**
     * 离线逻辑 买保险服务
     * @param uuid
     * @return
     */
    public static UserTag buyInsurance(String uuid) {
        String where = " app_goods_sn = 'ins1v1' and status = 1 and uuid = '" + uuid + "'";
        long count = ClickHouseUtil.getCountByCondition(TABLE_PAYMENT_ORDER, where);
        log.info("buyInsurance clickhouse count:" + count);
        return UserTag.builder().uuid(uuid).tagName(TAG_NAME_BUY_INSURANCE).tagType(TAG_TYPE_USER_TOUCH).haveTag(haveTag(count)).build();
    }
    /**
     * 离线逻辑 有效咨询
     * @param uuid
     * @return
     */
    public static UserTag effectiveAdvisory(String uuid) {
        String where = " event = 0 and content_type = 1 and uuid = " + uuid;
        long count = ClickHouseUtil.getCountByCondition(TABLE_WECHAT_EVENT,where);
        log.info("effectiveAdvisory clickhouse count:" + count);
        return UserTag.builder().uuid(uuid).tagName(TAG_NAME_EFFECTIVE_ADVISORY).tagType(TAG_TYPE_USER_TOUCH).haveTag(haveTag(count)).build();

    }
    private static boolean haveTag(long count) {
        //如果有该uuid记录，则说明该uuid
        return 1 <= count;
    }

    public static void main(String[] args) {
        System.out.println(JSON.toString(buyInsurance("446c037840abb29a70ac3501c7ceb2fa")));
    }
}
