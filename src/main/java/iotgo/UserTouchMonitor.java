package iotgo;

import iotgo.bean.UserTouchInfo;
import iotgo.util.processDataUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import ru.ivi.opensource.flinkclickhousesink.ClickhouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkConsts;

import java.util.*;

import static iotgo.util.Const.FLINK_PARALLELISM;
import static iotgo.util.Const.KAFKA_START_TIME;

@Slf4j
public class UserTouchMonitor {

    public static void main(String[] args) {

        HashSet<String> filterEventType = new HashSet<>();
        filterEventType.add("FLOW");
        filterEventType.add("subscribe");
        filterEventType.add("subscribe-nature");

        final String kafka_topic_in = "event-stream";
        final String kafka_topic_out = "action-stream";
        final String clickhouse_table = "user_touch_monitor.user_touch_info";


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "ckafka.xiaobangtouzi.com:9092");
        kafkaProps.put("group.id", "test-flink201908062111");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("auto.offset.reset", "latest");
//        props.put("auto.offset.reset", "earliest");

        // create clickhouse sink props for sink
        Properties clickhouseProps = new Properties();
        clickhouseProps.put(ClickhouseSinkConsts.TARGET_TABLE_NAME, clickhouse_table);
        clickhouseProps.put(ClickhouseSinkConsts.MAX_BUFFER_SIZE, "10000");

        Map<String, String> globalParameters = new HashMap<>();

        // clickhouse cluster properties
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, "172.16.200.16:8123");
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_USER, "");
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, "");

        // sink common
        globalParameters.put(ClickhouseSinkConsts.TIMEOUT_SEC, "10");
        globalParameters.put(ClickhouseSinkConsts.FAILED_RECORDS_PATH, "");
        globalParameters.put(ClickhouseSinkConsts.NUM_WRITERS, "10");
        globalParameters.put(ClickhouseSinkConsts.NUM_RETRIES, "10");
        globalParameters.put(ClickhouseSinkConsts.QUEUE_MAX_CAPACITY, "10");

        // set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(globalParameters);
        env.getConfig().setGlobalJobParameters(parameters);

        getKafkaByFlink(kafka_topic_in,env,kafkaProps,FLINK_PARALLELISM,KAFKA_START_TIME,filterEventType).
                addSink(new ClickhouseSink(clickhouseProps)).name("user_touch_info_in clickhouse sink");

        getKafkaByFlink(kafka_topic_out,env,kafkaProps,FLINK_PARALLELISM,KAFKA_START_TIME,filterEventType).
                addSink(new ClickhouseSink(clickhouseProps)).name("user_touch_info_out clickhouse sink");

        try {
            env.execute("test flink kafka sink clickhouse");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param kafkaTopic 处理 kafka topic
     * @param env flink 环境
     * @param kafkaProps kafka 配置
     * @param flinkParallelism flink 并行度
     * @param filterEventType kafka消息中过来eventType
     * @return
     */
    public static SingleOutputStreamOperator<String> getKafkaByFlink(
            String kafkaTopic,
            StreamExecutionEnvironment env,
            Properties kafkaProps,
            int flinkParallelism,
            long kafkaStartTime,
            HashSet<String> filterEventType) {
        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>(
                kafkaTopic,
                new SimpleStringSchema(),
                kafkaProps);

        if (0L != kafkaStartTime){
            consumer010.setStartFromTimestamp(kafkaStartTime);
        }
        return env.addSource(consumer010).setParallelism(flinkParallelism)
                //解析有用字段
                .map(s -> processDataUtil.parseUserTouchInfo(s,kafkaTopic))
                //过滤各个字段为null或者错误数据
                .filter(processDataUtil::checkUserTouchInfo)
                //过滤eventType不在列表中事件类型
                .filter(f -> filterEventType.contains(f.getEventType()))
                .map(UserTouchInfo::convertToCsv);
    }
}
