package iotgo.bean;

import lombok.*;

import java.util.HashMap;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MysqlBinlogInfo {

    private String database;
    private String table;
    private String type;
    private long ts;
    private long xid;
    private boolean commit;
    private HashMap<String,Object> data;
    private String kafkaTopic;

}