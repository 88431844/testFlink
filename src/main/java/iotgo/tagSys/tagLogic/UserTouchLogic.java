package iotgo.tagSys.tagLogic;

import iotgo.bean.UserTag;
import iotgo.util.ClickHouseUtil;

import static iotgo.util.Const.*;

/**
 * 用户触达系统 标签处理逻辑
 */
public class UserTouchLogic {

    public static UserTag addFriend(String uuid) {
        long i = ClickHouseUtil.getCount(TABLE_ACCOUNT_WECHAT_MATCH, uuid);
        boolean haveTag = false;
        //如果有该uuid记录，则说明该uuid已经 加/申请好友
        if (1 == i) {
            haveTag = true;
        }
        return UserTag.builder().uuid(uuid).tagName(TAG_NAME_ADD_FRIEND).tagType(TAG_TYPE_USER_TOUCH).haveTag(haveTag).build();
    }
}
