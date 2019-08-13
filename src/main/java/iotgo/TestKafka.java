package iotgo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import static iotgo.util.Const.KAFKA_START_TIME;
import static iotgo.util.KafkaUtil.getKafkaProperties;

public class TestKafka {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>(
                "event-stream",
                new SimpleStringSchema(),
                getKafkaProperties());

        consumer010.setStartFromTimestamp(KAFKA_START_TIME);


        SingleOutputStreamOperator<String> kafka_in = env.addSource(consumer010).setParallelism(1);
        kafka_in.print();

        try {
            env.execute("test flink kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
