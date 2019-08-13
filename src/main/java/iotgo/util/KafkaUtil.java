package iotgo.util;

import java.util.Properties;

public class KafkaUtil {

    public static Properties getKafkaProperties(){
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "ckafka.xiaobangtouzi.com:9092");
        kafkaProps.put("group.id", "test-flink201908062111");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("auto.offset.reset", "latest");
        return kafkaProps;
    }
}
