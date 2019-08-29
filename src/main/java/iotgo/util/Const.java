package iotgo.util;

public class Const {

    /**
     * flink 并行度
     */
    public static final int FLINK_PARALLELISM = 1;
    /**
     *  kafka 消费开始时间
     */
    public static final long KAFKA_START_TIME = 1565367042000L;
    /**
     * clickhoust hosts 地址
     */
    public static final String CLICKHOUSE_HOSTS = "172.16.200.16:8123";

    /**
     * kafka topic
     */
    //线上库220
    public final static String mysqlBinlogTopic_220 = "mysql-220";
    //线上库227
    public final static String mysqlBinlogTopic_227 = "mysql-227";
    //QA库
    public final static String mysqlBinlogTopic_115 = "mysql-115";
}
