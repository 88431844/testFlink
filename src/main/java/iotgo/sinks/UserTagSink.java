package iotgo.sinks;

import com.alibaba.fastjson.JSON;
import iotgo.bean.UserTag;
import iotgo.util.JedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ResourceBundle;

import static iotgo.util.Const.PROCESSED_TAG_REDIS_PREFIX;
import static iotgo.util.Const.TAG_TO_PROCESS_REDIS_PREFIX;
import static iotgo.util.StringUtil.getUserTagRedis;

/**
 * 标签sink
 * 将用户标签信息添加到
 */
@Slf4j
public class UserTagSink extends RichSinkFunction<UserTag> {
    private Connection connection = null;
    private PreparedStatement ps = null;
    private Jedis jedis;

    /**
     * 一、open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ResourceBundle rb = ResourceBundle.getBundle("mysql");
        String driver = rb.getString("driver");
        String url = rb.getString("url");
        String username = rb.getString("username");
        String password = rb.getString("password");
        //1.加载驱动
        Class.forName(driver);
        //2.创建连接
        connection = DriverManager.getConnection(url, username, password);
        String sql = "insert into user_tag(uuid,tag_name,tag_desc,tag_type,create_at,update_at) values(?,?,?,?,now(),now()) ON DUPLICATE KEY UPDATE update_at = now() ;";
        //3.获得执行语句
        ps = connection.prepareStatement(sql);

        jedis = JedisUtil.getJedis();
    }


    /**
     * 二、每个元素的插入都要调用一次invoke()方法，这里主要进行插入操作
     */
    @Override
    public void invoke(UserTag userTag) {
        log.info("-----------------------------------------");
        log.info("UserTagSink:" + JSON.toJSONString(userTag));
        //如果处理标签逻辑后，有该标签，则将标签信息存入MySql中，否则，只存入Redis中的处理标志位，并清除待处理标志。
        if (userTag.isHaveTag()) {
            try {
                //4.组装数据，执行插入操作
                ps.setString(1, userTag.getUuid());
                ps.setString(2, userTag.getTagName());
                ps.setString(3, userTag.getTagDesc());
                ps.setString(4, userTag.getTagType());
                ps.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        String userTagRedis = getUserTagRedis(userTag.getUuid(),userTag.getTagName());

        //将Redis中待处理标志位，清除
        String redisDelKey = TAG_TO_PROCESS_REDIS_PREFIX + userTagRedis;
        jedis.del(redisDelKey);
        log.info("redis del key:" + redisDelKey);

        //将标签信息存入MySql中；将已经计算用户标签标志位存入Redis中
        String redisSetKey = PROCESSED_TAG_REDIS_PREFIX + userTagRedis;
        jedis.set(redisSetKey, String.valueOf(System.currentTimeMillis()));
        log.info("redis set key:" + redisSetKey);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //5.关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }

        JedisUtil.close(jedis);
    }

}