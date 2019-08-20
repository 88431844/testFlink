package iotgo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import iotgo.bean.MysqlBinlogInfo;
import iotgo.util.CommonUtil;
import iotgo.util.Const;
import iotgo.util.HttpUtil;
import iotgo.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;

public class MySqlBinlogWatcher {

    //线上库
    private final static String mysqlBinlogTopic = "mysql-220";
    //QA库
//    private final static String mysqlBinlogTopic = "mysql-115";
    private final static String kafkaGroupId = "mysqlBinlogWatcher_groupId_001";
    private final static String flinkJobName = "mysqlBinlogWatcher";
    //数据团队机器人
    private final static String wxRobotUrl = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=1b8cd3fc-3778-4c22-a865-15fab45f6816";
    //测试机器人
//    private final static String wxRobotUrl = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=dc9adabc-def3-4bf3-9662-c619a57bad81";


    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        HashSet<String> mysqlDDLfilter = new HashSet<>();
        mysqlDDLfilter.add("table-create");
        mysqlDDLfilter.add("table-alter");
        mysqlDDLfilter.add("table-drop");
//        mysqlDDLfilter.add("insert");
//        mysqlDDLfilter.add("update");

        KafkaUtil.getKafkaByFlink(
                mysqlBinlogTopic,
                env,
                KafkaUtil.getKafkaProperties(kafkaGroupId),
                Const.FLINK_PARALLELISM,
                CommonUtil.getArgsTimeStamp(args))
//                1566290608000L)
                //Json to bean
                .map(m -> JSON.parseObject(m, MysqlBinlogInfo.class))
                //过滤出目标事件
                .filter(m -> mysqlDDLfilter.contains(m.getType()))
                //请求HTTP 企业微信机器人
                .map(m -> HttpUtil.doPost(wxRobotUrl, getReqJson(m)));
//                .print();
        try {
            env.execute(flinkJobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static JSONObject getReqJson(MysqlBinlogInfo m) {
        String content = JSON.toJSONString(m)
                .replace("\"", "")
                .replace("\\", "");
        return JSON.parseObject("{\"msgtype\":\"text\",\"text\":{\"content\":\"" + content + "\"}}");
    }
}
