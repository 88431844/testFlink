package iotgo;

import iotgo.bean.UserTag;
import iotgo.sinks.UserTagSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySql_UserTag_Sink_test {
    public static void main(String[] args) {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //2.准备数据
        DataStream<UserTag> userTagDataStreamSource = env.fromElements(
                UserTag.builder().uuid("1").tagName("标签1").tagDesc("这是标签1").tagType("触达系统1").build(),
                UserTag.builder().uuid("2").tagName("标签2").tagDesc("这是标签2").tagType("触达系统2").build()
        );

        //3.将数据写入到自定义的sink中（这里是mysql）
        userTagDataStreamSource.addSink(new UserTagSink());

        try {
            //4.触发流执行
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
