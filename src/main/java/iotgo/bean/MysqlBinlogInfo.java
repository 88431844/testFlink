package iotgo.bean;

import lombok.*;

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
    private String data;
    private String kafkaTopic;

}