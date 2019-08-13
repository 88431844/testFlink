package iotgo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import iotgo.bean.UserTouchInfo;
import org.apache.commons.lang3.StringUtils;

public class ProcessDataUtil {

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
        //subscribe无下面两个字段，故去掉下面两个判断
//        else if (0L == userTouchInfo.getFlowId()){
//            return false;
//        }
//        else if (0L == userTouchInfo.getNodeTypeId()){
//            return false;
//        }
        else {
            return true;
        }
    }
    public static UserTouchInfo parseUserTouchInfo(String s,String kafkaTopic){
        JSONObject jsonObject = JSON.parseObject(s);
        JSONObject dataJson = JSON.parseObject(String.valueOf(jsonObject.get("data")));
        return UserTouchInfo.builder()
                .userId(ProcessDataUtil.processString(jsonObject,"userId"))
                .eventTime(ProcessDataUtil.processLong(jsonObject,"eventTime"))
                .eventType(ProcessDataUtil.processString(jsonObject,"eventType"))
                .channel(ProcessDataUtil.processString(jsonObject,"channel"))
                .uuid(ProcessDataUtil.processString(dataJson,"uuid"))
                .openId(ProcessDataUtil.processString(dataJson,"openId"))
                .flowId(ProcessDataUtil.processLong(dataJson,"flowId"))
                .nodeTypeId(ProcessDataUtil.processLong(dataJson,"nodeTypeId"))
                .kafkaTopic(kafkaTopic)
                .build();
    }
}
