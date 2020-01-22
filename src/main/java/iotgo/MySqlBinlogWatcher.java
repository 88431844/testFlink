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

import static iotgo.util.Const.mysqlBinlogTopic_220;
import static iotgo.util.Const.mysqlBinlogTopic_227;

public class MySqlBinlogWatcher {


    private final static String kafkaGroupId = "mysqlBinlogWatcher_groupId_001";
    private final static String flinkJobName = "mysqlBinlogWatcher";
    //数据团队机器人
    private final static String wxRobotUrl = "";
    //测试机器人
//    private final static String wxRobotUrl = "";


    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        HashSet<String> mysqlDDLfilter = new HashSet<>();
        mysqlDDLfilter.add("table-create");
        mysqlDDLfilter.add("table-alter");
        mysqlDDLfilter.add("table-drop");
//        mysqlDDLfilter.add("insert");
//        mysqlDDLfilter.add("update");

        //220监控
        watcher(mysqlBinlogTopic_220,kafkaGroupId,env,args,mysqlDDLfilter);
        //227监控
        watcher(mysqlBinlogTopic_227,kafkaGroupId,env,args,mysqlDDLfilter);

        try {
            env.execute(flinkJobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 拼接请求json
     * @param m mysql binlog 内容
     * @return
     */
    private static JSONObject getReqJson(MysqlBinlogInfo m) {
        String content = JSON.toJSONString(m)
                .replace("\"", "")
                .replace("\\", "");
        return JSON.parseObject("{\"msgtype\":\"text\",\"text\":{\"content\":\"" + content + "\"}}");
    }

    /**
     * mysql binlog 监控
     * @param kafkaTopic
     * @param kafkaGroupId
     * @param env
     * @param args
     * @param mysqlDDLfilter
     */
    private static void watcher(
            String kafkaTopic,
            String kafkaGroupId,
            StreamExecutionEnvironment env,
            String[] args,
            HashSet<String> mysqlDDLfilter) {
        KafkaUtil.getKafkaByFlink(
                kafkaTopic,
                env,
                KafkaUtil.getKafkaProperties(kafkaGroupId),
                Const.FLINK_PARALLELISM,
                CommonUtil.getArgsTimeStamp(args))
//                1566290608000L)
                //Json to bean
                .map(m -> parseBean(m,kafkaTopic))
                //过滤出目标事件
                .filter(m -> mysqlDDLfilter.contains(m.getType()))
                //请求HTTP 企业微信机器人
                .map(m -> HttpUtil.doPost(wxRobotUrl, getReqJson(m)));
//                .print();
    }

    /**
     * 将Kafka中json 转换为bean
     * @param json
     * @param kafkaTopic
     * @return
     */
    private static MysqlBinlogInfo parseBean(String json, String kafkaTopic){
        MysqlBinlogInfo mysqlBinlogInfo = JSON.parseObject(json,MysqlBinlogInfo.class);
        if (null != mysqlBinlogInfo){
            mysqlBinlogInfo.setKafkaTopic(kafkaTopic);
        }
        return mysqlBinlogInfo;

    }
}
