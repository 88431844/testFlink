package iotgo;

import com.alibaba.fastjson.JSON;
import iotgo.util.Const;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ResourceBundle;

public class Test {

    public static void main(String[] args) {

        String s = "{\"database\":\"user_center\",\"table\":\"account_aochuang_reverse_manual_match\",\"type\":\"insert\",\"ts\":1567066541,\"xid\":1188058004,\"commit\":true,\"data\":{\"id\":395340,\"uuid\":\"5c9458fb5e7b43a3bcaa758a3cc51484\",\"headimgurl\":\"http://thirdwx.qlogo.cn/mmopen/hgXWbMaaqmBMW7MtdhNsgk8f2oLMkB52X5xyjFjtL6AjlA6TYwP19jovmgbGU43HUicibhT3hiaNlw2kmYryhtWsVVUT69nn5Xic/132\",\"aochuang_wechat_friend_id\":141457,\"aochuang_account_id\":0,\"aochuang_wechat_account_id\":278380,\"aochuang_wechat_id\":\"wxid_skuzwt39qqzm22\",\"aochuang_nickname\":\"Y\",\"aochuang_avatar\":\"\",\"aochuang_gender\":1,\"aochuang_country\":\"中国\",\"aochuang_province\":\"广东\",\"aochuang_city\":\"广州\",\"aochuang_created_at\":1555991145,\"aochuang_updated_at\":1556032069,\"weight\":150,\"match_avatar_score\":0.0,\"created_at\":1567066540,\"updated_at\":1567066540,\"data_source\":0}}";
//        JSONObject jsonObject = JSON.parseObject(s);
//        System.out.println("database:" + jsonObject.get("database"));
//        System.out.println("table:"+ jsonObject.get("table"));
//        System.out.println("type:" + jsonObject.get("type"));


//        String a = "10086_BUY_CLASS";
//        String a = "10086_BUY_CLASS_NOT";
//        String b[] = a.split("_", 2);
//        for (String c : b
//        ) {
//            System.out.println(c);
//        }
//        Connection connection = null;
//        PreparedStatement ps = null;
//        ResourceBundle rb = ResourceBundle.getBundle("mysql");
//        String driver = rb.getString("driver");
//        String url = rb.getString("url");
//        String username = rb.getString("username");
//        String password = rb.getString("password");
//
//        try {
//            //1.加载驱动
//            Class.forName(driver);
//            //2.创建连接
//            connection = DriverManager.getConnection(url, username, password);
//            String sql = "insert into user_tag(uuid,tag_name,tag_desc,tag_type,create_at,update_at) " +
//                    "values(?,?,?,?,now(),now()) ON DUPLICATE KEY UPDATE update_at = now() ;";
//            //3.获得执行语句
//            ps = connection.prepareStatement(sql);
//            //4.组装数据，执行插入操作
//            ps.setString(1, "10086");
//            ps.setString(2, "BUY_CLASS");
//            ps.setString(3, "");
//            ps.setString(4, Const.TAG_TYPE_USER_TOUCH);
//            ps.executeUpdate();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("done ...");

    }
}
