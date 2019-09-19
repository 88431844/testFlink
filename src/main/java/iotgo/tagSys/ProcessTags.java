package iotgo.tagSys;

import iotgo.bean.UserTag;
import iotgo.sinks.UserTagSink;
import iotgo.tagSys.realtimeLogic.ProcessAddFriend;
import iotgo.tagSys.realtimeLogic.ProcessBuyInsurance;
import iotgo.tagSys.realtimeLogic.ProcessEffectiveAdvisory;
import iotgo.util.Const;
import iotgo.util.FilterUtil;
import iotgo.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static iotgo.util.CommonUtil.getArgsTimeStamp;
import static iotgo.util.Const.*;
import static iotgo.util.FilterUtil.filterMySqlBinlog;
import static iotgo.util.FilterUtil.filterMySqlBinlogSub;
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

        /**
         * 处理 买保险服务标签 逻辑
         */
        ProcessBuyInsurance processBuyInsurance = new ProcessBuyInsurance();
        processBuyInsurance.process(originalStreamStr_220).addSink(new UserTagSink());
        /**
         * 处理 加好友标签 逻辑
         */
        ProcessAddFriend processAddFriend = new ProcessAddFriend();
        processAddFriend.process(originalStreamStr_220).addSink(new UserTagSink());
        /**
         * 处理 购买财商课标签 逻辑
         */


        /**
         * 处理 咨询过标签 逻辑
         */
        ProcessEffectiveAdvisory processEffectiveAdvisory = new ProcessEffectiveAdvisory();
        processEffectiveAdvisory.process(originalStreamStr_220).addSink(new UserTagSink());

        try {
            env.execute("ProcessTags");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
