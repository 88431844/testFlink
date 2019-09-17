package iotgo.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;
@Slf4j
public class KafkaUtil {

    /**
     * 获取kafka配置
     * @param kafkaGroupId groupId
     * @return
     */
    public static Properties getKafkaProperties(String kafkaGroupId){
        if (StringUtils.isEmpty(kafkaGroupId)){
            kafkaGroupId = "UserTouchMonitor_" + System.currentTimeMillis();
        }
        log.info("getKafkaProperties kafkaGroupId:" + kafkaGroupId);
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "ckafka.xiaobangtouzi.com:9092");
//        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", kafkaGroupId);
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("auto.offset.reset", "earliest");
        return kafkaProps;
    }

    /**
     * flink读取kafka
     * @param kafkaTopic       处理 kafka topic
     * @param env              flink 环境
     * @param kafkaProps       kafka 配置
     * @param flinkParallelism flink 并行度
     * @return
     */
    public static SingleOutputStreamOperator<String> getKafkaByFlink(
            String kafkaTopic,
            StreamExecutionEnvironment env,
            Properties kafkaProps,
            int flinkParallelism,
            long kafkaStartTime) {
        log.info("getKafkaByFlink kafkaStartTime :" + kafkaStartTime );

        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>(
                kafkaTopic,
                new SimpleStringSchema(),
                kafkaProps);

        if (0L != kafkaStartTime) {
            consumer010.setStartFromTimestamp(kafkaStartTime);
        }
        return env.addSource(consumer010).setParallelism(flinkParallelism);
    }
}
