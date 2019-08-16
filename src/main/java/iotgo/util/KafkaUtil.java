package iotgo.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;
@Slf4j
public class KafkaUtil {

    public static Properties getKafkaProperties(String kafkaGroupId){
        if (StringUtils.isEmpty(kafkaGroupId)){
            kafkaGroupId = "UserTouchMonitor_" + System.currentTimeMillis();
        }
        log.info("getKafkaProperties kafkaGroupId:" + kafkaGroupId);
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "ckafka.xiaobangtouzi.com:9092");
        kafkaProps.put("group.id", kafkaGroupId);
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("auto.offset.reset", "latest");
        return kafkaProps;
    }
}
