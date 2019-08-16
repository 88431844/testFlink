package iotgo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import iotgo.bean.UserTouchInfo;
import org.apache.commons.lang3.StringUtils;

public class ProcessDataUtil {

    public static String processString(JSONObject jsonObject,String key){
        if (null == jsonObject){
            return "";
        }
        Object o = jsonObject.get(key);
        if (null == o){
            return "";
        }else {
            return String.valueOf(o);
        }
    }

    public static Long processLong(JSONObject jsonObject,String key){
        if (null == jsonObject){
            return 0L;
        }
        String value = processString(jsonObject,key);
        if (StringUtils.isEmpty(value)){
            return 0L;
        }else {
            return Long.parseLong(value);
        }
    }
    @Deprecated
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
//        else if (StringUtils.isEmpty(userTouchInfo.getOpenId())){
//            return false;
//        }
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
        JSONObject actionJson = JSON.parseObject(String.valueOf(jsonObject.get("action")));
        return UserTouchInfo.builder()
                .userId(ProcessDataUtil.processString(jsonObject,"userId"))
                .eventTime(ProcessDataUtil.processLong(jsonObject,"eventTime"))
                .eventType(ProcessDataUtil.processString(jsonObject,"eventType"))
                .channel(ProcessDataUtil.processString(jsonObject,"channel"))
                .uuid(ProcessDataUtil.processString(dataJson,"uuid"))
                .openId(ProcessDataUtil.processString(dataJson,"openId"))
                .flowId(ProcessDataUtil.processLong(dataJson,"flowId"))
                .nextFlowId(ProcessDataUtil.processString(actionJson,"nextFlowId"))
                .nodeTypeId(ProcessDataUtil.processLong(dataJson,"nodeTypeId"))
                .kafkaTopic(kafkaTopic)
                .build();
    }

    public static void main(String[] args) {
        String s = "{\"userId\":8214698186,\"eventTime\":1565687672558,\"eventType\":\"FLOW\",\"channel\":\"wxd2548b34c19953a7\",\"data\":{\"unionId\":\"oJAmzwHnKumKtAA5b6lAkVG7PbqY\",\"flowNodeId\":\"14778_13\",\"openId\":\"oghSu1cyfuBIv-7CaAZff4RkMt7c\",\"ip\":\"36.46.23.192\",\"ua\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G60 MicroMessenger/7.0.4(0x17000428) NetType/4G Language/zh_CN\",\"uuid\":\"3067909dd58b46a39c9c252e44cbd88b\",\"flowId\":14778,\"nodeTypeId\":13,\"value\":\"\"},\"action\":{\"eventType\":\"CUSTOM\",\"nextFlowId\":\"13392\"}}";
        System.out.println(parseUserTouchInfo(s,"test"));
    }
}
