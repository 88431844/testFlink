package iotgo;

import iotgo.util.Const;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import static iotgo.util.Const.KAFKA_START_TIME;
import static iotgo.util.KafkaUtil.getKafkaProperties;
@Deprecated
public class TestKafka {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>(
//                Const.mysqlBinlogTopic_220,
                Const.TAG_TO_PROCESS_KAFKA_TOPIC,
//                "event-stream",
                new SimpleStringSchema(),
                getKafkaProperties("test_flink2"));

//        long seventDay = 1564650773000L;
//        consumer010.setStartFromTimestamp(seventDay);


        SingleOutputStreamOperator<String> kafka_in = env.addSource(consumer010).setParallelism(1);
//                .filter(k -> StringUtils.contains(k,"ACCOUNT_WECHAT_MATCH"));
        kafka_in.print();

        try {
            env.execute("test flink kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
