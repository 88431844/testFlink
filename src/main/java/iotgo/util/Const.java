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

    /**
     * 用户标签系统相关常量
     */
    /**
     * 数据库名称
     */
    //user_center 数据库
    public final static String DATABASE_USER_CENTER = "user_center";

    /**
     * 表名称
     */
    //account_wechat_match 表
    public final static String TABLE_ACCOUNT_WECHAT_MATCH = "account_wechat_match";

    /**
     * 表操作
     */
    //添加 操作
    public final static String OP_TYPE_INSERT = "insert";
    //更新 操作
    public final static String OP_TYPE_UPDATE = "update";

    /**
     * 标签类型
     */
    //触达系统
    public final static String TAG_TYPE_USER_TOUCH = "USER_TOUCH";

    /**
     * 标签名称
     */
    //添加好友
    public final static String TAG_NAME_ADD_FRIEND = "ADD_FRIEND";
}
