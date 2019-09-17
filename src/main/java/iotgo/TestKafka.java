package iotgo;

import com.alibaba.fastjson.JSON;
import iotgo.util.Const;
import iotgo.util.FilterUtil;
import iotgo.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import static iotgo.util.Const.KAFKA_START_TIME;
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

        long seventDay = 1568356471000L;
        consumer010.setStartFromTimestamp(seventDay);


        SingleOutputStreamOperator<String> kafka_in = env.addSource(consumer010).setParallelism(1);
//        kafka_in.map(m ->{
//            System.out.println("kafka_in:" + m);
//            return "";
//        });
        FilterUtil.filterMySqlBinlogSub(kafka_in, "payment", "payment_order")
//                        .filter(f -> f.getData().get("app_goods_sn").equals("ins1v1") || f.getData().get("status").equals("1"))
                .filter(f -> isNotEmpty(String.valueOf(f.getData().get("trade_info")))
                        && isNotEmpty(String.valueOf(f.getData().get("status")))
                        && isNotEmpty(String.valueOf(f.getData().get("app_goods_sn"))))
                .map(m -> {
                    System.out.println("m:" + m);
                    String status = String.valueOf(m.getData().get("status"));
                    String app_goods_sn = String.valueOf(m.getData().get("app_goods_sn"));
                    System.out.println("status:" + status + ",app_goods_sn:" + app_goods_sn);
                    return "";
                });
//                        .print();
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
