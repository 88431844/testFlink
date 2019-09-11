package iotgo.tagSys;

import iotgo.bean.UserTag;
import iotgo.sinks.UserTagSink;
import iotgo.tagSys.tagLogic.UserTouchLogic;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static iotgo.util.Const.*;
import static iotgo.util.KafkaUtil.getKafkaByFlink;
import static iotgo.util.KafkaUtil.getKafkaProperties;

/**
 * 处理有逻辑 未计算标签
 * 1.消费Kafka中的未处理标签信息
 * 2.根据标签处理逻辑，进行处理
 * 3.将Redis中待处理标志位，清除；将标签信息存入MySql中；将已经计算用户标签标志位存入Redis中
 */
@Slf4j
public class ProcessQueue {

    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        getKafkaByFlink(TAG_TO_PROCESS_KAFKA_TOPIC, env, getKafkaProperties("flink_ProcessQueue"), 1, 0L)
                .map(m -> {
                    String uuid = m.split("_", 2)[0];
                    String tagName = m.split("_", 2)[1];
                    System.out.println("uuid:" + uuid + ",tagName:" + tagName);
                    UserTag userTag;
                    //根据标签，switch到各个标签计算逻辑分支
                    switch (tagName) {
                        case TAG_NAME_ADD_FRIEND:
                            userTag = UserTouchLogic.addFriend(uuid);
                            break;
                        default:
                            userTag = UserTag.builder().uuid(null).build();
                    }
                    return userTag;
                })
                //过滤default情况
                .filter(f -> null != f.getUuid())
                .addSink(new UserTagSink());
        try {
            env.execute("ProcessQueue");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
