package iotgo.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ResourceBundle;

public final class JedisUtil {
    private JedisUtil() {
    }

    private static JedisPool jedisPool;
    private static int maxtotal;
    private static int maxwaitmillis;
    private static String host;
    private static int port;
    private static int database;
    private static int timeout;
    private static String password;

    /*读取jedis.properties配置文件*/
    static {
        ResourceBundle rb = ResourceBundle.getBundle("jedis");
        maxtotal = Integer.parseInt(rb.getString("maxtotal"));
        maxwaitmillis = Integer.parseInt(rb.getString("maxwaitmillis"));
        host = rb.getString("host");
        password = rb.getString("password");
        port = Integer.parseInt(rb.getString("port"));
        database = Integer.parseInt(rb.getString("database"));
        timeout = Integer.parseInt(rb.getString("timeout"));
    }

    /*创建连接池*/
    static {
        GenericObjectPoolConfig jedisPoolConfig = new GenericObjectPoolConfig();
        jedisPoolConfig.setMaxTotal(maxtotal);
        jedisPoolConfig.setMaxWaitMillis(maxwaitmillis);
        jedisPoolConfig.setTestOnCreate(false);
        jedisPool = new JedisPool(jedisPoolConfig, host, port, timeout, password, database);
    }

    /*获取jedis*/
    public static Jedis getJedis() {
        return jedisPool.getResource();
    }

    /*关闭Jedis*/
    public static void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }


}
