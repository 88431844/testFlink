package iotgo.tagSys.realtimeLogic.impl;

import iotgo.bean.MysqlBinlogInfo;
import iotgo.bean.UserTag;
import iotgo.tagSys.dimensionTable.TestTable;
import iotgo.tagSys.realtimeLogic.ProcessTagIF;
import org.apache.flink.api.java.io.jdbc.JDBCLookupOptions;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class ProcessBuyFBCourse implements ProcessTagIF {
    @Override
    public SingleOutputStreamOperator<UserTag> process(SingleOutputStreamOperator<String> sso, StreamTableEnvironment tableEnvironment) {
        JDBCTableSource.Builder  orderGoodsSkuBuild= TestTable.getTableBuild("order_center","order_goods_sku");
        orderGoodsSkuBuild.setLookupOptions(JDBCLookupOptions.builder()
                .setCacheMaxSize(1000).setCacheExpireMs(1000).setMaxRetryTimes(30).build());

        tableEnvironment.registerFunction("order_goods_sku",
                orderGoodsSkuBuild.build().getLookupFunction(new String[]{"order_sn"}));



        //getOrderSn
        //getOrderGoodsSku
        return null;
    }

    @Override
    public boolean filter(MysqlBinlogInfo mysqlBinlogInfo) {
        return false;
    }
}
