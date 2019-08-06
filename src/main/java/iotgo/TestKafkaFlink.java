package iotgo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import java.util.Properties;

public class TestKafkaFlink {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "ckafka.xiaobangtouzi.com:9092");
        props.put("zookeeper.connect", "ckafka.xiaobangtouzi.com:2181");
        props.put("group.id", "test-flink201908062111");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
//        props.put("auto.offset.reset", "earliest");

        SingleOutputStreamOperator<String> kafka = env.addSource(new FlinkKafkaConsumer010<>(
                "event-stream-qa",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(s -> "log:" + s);

//        student.addSink(new SinkToMySQL()); //数据 sink 到 mysql
        kafka.print();

        try {
            env.execute("test flink kafka");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
