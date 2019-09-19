package iotgo;

import com.alibaba.fastjson.JSON;
import iotgo.bean.MysqlBinlogInfo;
import iotgo.util.Const;
import iotgo.util.FilterUtil;
import iotgo.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.HashMap;

import static iotgo.util.Const.*;
import static iotgo.util.FilterUtil.filterMySqlBinlogSub;
import static iotgo.util.KafkaUtil.getKafkaProperties;
import static iotgo.util.StringUtil.isNotEmpty;

@Deprecated
public class TestKafka {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>(
                Const.mysqlBinlogTopic_220,
//                Const.TAG_TO_PROCESS_KAFKA_TOPIC,
//                "event-stream",
                new SimpleStringSchema(),
                getKafkaProperties("test_flink3"));

        //设置从哪天开始消费Kafka
//        long startDay = 1568736054000L;
//        consumer010.setStartFromTimestamp(startDay);


        SingleOutputStreamOperator<String> kafka_in = env.addSource(consumer010).setParallelism(1);
//        filterMySqlBinlogSub(kafka_in,DATABASE_PAYMENT,TABLE_PAYMENT_ORDER)
//                .filter(f ->
//                    "ins1v1".equals(String.valueOf(f.getData().get("app_goods_sn")))
//                )
//                .filter(new FilterBuyInsurance())
//                .filter(f -> {
//                    Object o = f.getData().get("status");
//                    if (null != o){
//                        return 1 == Integer.parseInt(String.valueOf(o));
//                    }
//                    return false;
//                })
//                .filter(f ->{
//                    boolean flag = false;
//                    HashMap<String,Object> data = f.getData();
//                    if (null != data && data.size() > 0){
//                        if (null != data.get("event") && null != data.get("content_type")){
////                            return data.get("event").equals("0") && data.get("content_type").equals("1");
//                            flag = String.valueOf(data.get("content_type")).equals("1");
//                        }
//                    }
//                    System.out.println("flag:"+flag);
//                    return flag;
//                })
//                .filter(f ->{
//                    MysqlBinlogInfo mysqlBinlogInfo = JSON.parseObject(f, MysqlBinlogInfo.class);
//                    return mysqlBinlogInfo.getData().get("database").equals("user_center")
//                            && mysqlBinlogInfo.getData().get("table").equals("wechat_event")
//                            && mysqlBinlogInfo.getData().get("type").equals("insert");
//
//                })
//                .print();

//        kafka_in.map(m ->{
//            System.out.println("kafka_in:" + m);
//            return "";
//        });
//        FilterUtil.filterMySqlBinlog(kafka_in, "user_center", "wechat_event", OP_TYPE_INSERT)

//                .filter(f -> {
//                    if (StringUtil.isNotEmpty(String.valueOf(f.getData().get("content_type")))
//                            && StringUtil.isNotEmpty(String.valueOf(f.getData().get("event")))
//                            && StringUtil.isNotEmpty(String.valueOf(f.getData().get("uuid")))
//                    ){
//                        return f.getData().get("uuid").equals("46087e077d174a258c09c79b13d75c2f");
////                                && f.getData().get("content_type").equals("1")
////                                && f.getData().get("event").equals("0");
//                    }
//                    return false;
//
//                })

//                .filter(f ->{
//                    //where event = 0 and content_type = 1 and open_id =
//                    return f.getData().get("content_type").equals("1") && f.getData().get("event").equals("0");
//                })

        FilterUtil.filterMySqlBinlogSub(kafka_in, "user_center", "wechat_event")
                .print();
//                        .filter(f -> f.getData().get("app_goods_sn").equals("ins1v1") || f.getData().get("status").equals("1"))
//                .filter(f -> isNotEmpty(String.valueOf(f.getData().get("trade_info")))
//                        && isNotEmpty(String.valueOf(f.getData().get("status")))
//                        && isNotEmpty(String.valueOf(f.getData().get("app_goods_sn"))))
//                .map(m -> {
//                    System.out.println("m:" + m);
//                    String status = String.valueOf(m.getData().get("status"));
//                    String app_goods_sn = String.valueOf(m.getData().get("app_goods_sn"));
//                    System.out.println("status:" + status + ",app_goods_sn:" + app_goods_sn);
//                    return "";
//                });
//                .print();
//        kafka_in.print();
//        FilterUtil.filterMySqlBinlog(kafka_in,)
//        kafka_in.map(m ->{
//            if (m.contains("insert")){
//                System.out.println();
//            }
//        })

        try {
            env.execute("test flink kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
