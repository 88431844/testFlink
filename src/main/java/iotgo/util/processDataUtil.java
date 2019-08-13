package iotgo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import iotgo.bean.UserTouchInfo;
import org.apache.commons.lang3.StringUtils;

public class processDataUtil {

    public static String processString(JSONObject jsonObject,String key){
        Object o = jsonObject.get(key);
        if (null == o){
            return "";
        }else {
            return String.valueOf(o);
        }
    }

    public static Long processLong(JSONObject jsonObject,String key){
        String value = processString(jsonObject,key);
        if (StringUtils.isEmpty(value)){
            return 0L;
        }else {
            return Long.parseLong(value);
        }
    }
    public static boolean checkUserTouchInfo(UserTouchInfo userTouchInfo){
        if (StringUtils.isEmpty(userTouchInfo.getUserId())){
            return false;
        }
        else if (0L == userTouchInfo.getEventTime()){
            return false;
        }
        else if (StringUtils.isEmpty(userTouchInfo.getEventType())){
            return false;
        }
        else if (StringUtils.isEmpty(userTouchInfo.getChannel())){
            return false;
        }
        else if (StringUtils.isEmpty(userTouchInfo.getUuid())){
            return false;
        }
        else if (StringUtils.isEmpty(userTouchInfo.getOpenId())){
            return false;
        }
        else if (0L == userTouchInfo.getFlowId()){
            return false;
        }
        else if (0L == userTouchInfo.getNodeTypeId()){
            return false;
        }
        else {
            return true;
        }
    }
    public static UserTouchInfo parseUserTouchInfo(String s,String kafkaTopic){
        JSONObject jsonObject = JSON.parseObject(s);
        JSONObject dataJson = JSON.parseObject(String.valueOf(jsonObject.get("data")));
        return UserTouchInfo.builder()
                .userId(processDataUtil.processString(jsonObject,"userId"))
                .eventTime(processDataUtil.processLong(jsonObject,"eventTime"))
                .eventType(processDataUtil.processString(jsonObject,"eventType"))
                .channel(processDataUtil.processString(jsonObject,"channel"))
                .uuid(processDataUtil.processString(dataJson,"uuid"))
                .openId(processDataUtil.processString(dataJson,"openId"))
                .flowId(processDataUtil.processLong(dataJson,"flowId"))
                .nodeTypeId(processDataUtil.processLong(dataJson,"nodeTypeId"))
                .kafkaTopic(kafkaTopic)
                .build();
    }
}
