package iotgo.tagSys;

import iotgo.bean.UserTag;
import iotgo.sinks.MySql_UserTag_Sink;
import iotgo.util.Const;
import iotgo.util.FilterUtil;
import iotgo.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
                .getKafkaByFlink(Const.mysqlBinlogTopic_220, env, getKafkaProperties("ProcessTags20190829"), FLINK_PARALLELISM, getArgsTimeStamp(args));

        /**
         * 处理 加好友标签 逻辑
         */
        FilterUtil.filterMySqlBinlog(originalStreamStr_220, DATABASE_USER_CENTER, TABLE_ACCOUNT_WECHAT_MATCH, OP_TYPE_INSERT)
                .map(m -> UserTag.builder()
                        .uuid(String.valueOf(m.getData().get("uuid")))
                        .tagName(TAG_NAME_ADD_FRIEND)
                        .tagType(TAG_TYPE_USER_TOUCH)
                        .tagDesc("")
                        .build())
                .addSink(new MySql_UserTag_Sink());
//        /**
//         * 处理 购买财商课标签 逻辑
//         */
//        originalStreamStr_220.filter(f -> FilterUtil.filterDatabase(f,"xxl-job")).map(m -> "BBBB:"+m).print();


        try {
            env.execute("ProcessTags");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}