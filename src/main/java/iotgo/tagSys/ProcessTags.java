package iotgo.tagSys;

import iotgo.sinks.UserTagSink;
import iotgo.tagSys.realtimeLogic.impl.ProcessAddFriend;
import iotgo.tagSys.realtimeLogic.impl.ProcessBuyFBCourse;
import iotgo.tagSys.realtimeLogic.impl.ProcessBuyInsurance;
import iotgo.tagSys.realtimeLogic.impl.ProcessEffectiveAdvisory;
import iotgo.util.Const;
import iotgo.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import static iotgo.util.CommonUtil.getArgsTimeStamp;
import static iotgo.util.Const.*;
import static iotgo.util.KafkaUtil.getKafkaProperties;

/**
 * 标签逻辑处理 flink job
 */
@Slf4j
public class ProcessTags {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //mysql_220 binlog
        SingleOutputStreamOperator<String> originalStreamStr_220 = KafkaUtil
                .getKafkaByFlink(Const.mysqlBinlogTopic_220, env, getKafkaProperties("ProcessTags20190917"), FLINK_PARALLELISM, getArgsTimeStamp(args));


        //指定blink
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(execEnv, settings);


        /**
         * 处理 买保险服务标签 逻辑
         */
        ProcessBuyInsurance processBuyInsurance = new ProcessBuyInsurance();
        processBuyInsurance.process(originalStreamStr_220,tableEnvironment).addSink(new UserTagSink());
        /**
         * 处理 加好友标签 逻辑
         */
        ProcessAddFriend processAddFriend = new ProcessAddFriend();
        processAddFriend.process(originalStreamStr_220,tableEnvironment).addSink(new UserTagSink());
        /**
         * 处理 购买财商课标签 逻辑
         */
        ProcessBuyFBCourse processBuyFBCourse = new ProcessBuyFBCourse();
        processBuyFBCourse.process(originalStreamStr_220,tableEnvironment).addSink(new UserTagSink());

        /**
         * 处理 咨询过标签 逻辑
         */
        ProcessEffectiveAdvisory processEffectiveAdvisory = new ProcessEffectiveAdvisory();
        processEffectiveAdvisory.process(originalStreamStr_220,tableEnvironment).addSink(new UserTagSink());

        try {
            env.execute("ProcessTags");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
