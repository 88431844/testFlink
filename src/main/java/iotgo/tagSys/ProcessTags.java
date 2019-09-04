package iotgo.tagSys;

import iotgo.util.Const;
import iotgo.util.FilterUtil;
import iotgo.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static iotgo.util.CommonUtil.getArgsTimeStamp;
import static iotgo.util.Const.FLINK_PARALLELISM;
import static iotgo.util.KafkaUtil.getKafkaProperties;

/**
 * 标签逻辑处理 flink job
 */
@Slf4j
public class ProcessTags {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //mysql_220 binlog
        SingleOutputStreamOperator<String> originalStreamStr_220 = KafkaUtil.getKafkaByFlink(Const.mysqlBinlogTopic_220,env,getKafkaProperties("ProcessTags20190829"),FLINK_PARALLELISM,getArgsTimeStamp(args));
        originalStreamStr_220.print();

        /**
         * 处理 加好友标签 逻辑
         */
        originalStreamStr_220.filter(f -> FilterUtil.filterDatabase(f,"administration")).map(m -> "AAAA:"+m).print();
        /**
         * 处理 购买财商课标签 逻辑
         */
        originalStreamStr_220.filter(f -> FilterUtil.filterDatabase(f,"xxl-job")).map(m -> "BBBB:"+m).print();


        try {
            env.execute("ProcessTags");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
