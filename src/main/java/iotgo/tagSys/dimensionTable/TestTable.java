package iotgo.tagSys.dimensionTable;

import org.apache.flink.api.java.io.jdbc.JDBCLookupOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

import java.util.ResourceBundle;

public class TestTable {

    public static JDBCTableSource.Builder getTableBuild(String database,String table) {
        ResourceBundle rb = ResourceBundle.getBundle("dimensionTable");

        String url = rb.getString(database + ".url");
        String driver = rb.getString(database + ".driver");
        String username = rb.getString(database + ".username");
        String password = rb.getString(database + ".password");
        String fields = rb.getString(database+"."+table + ".fields");
        String types = rb.getString(database+"."+table + ".types");

        JDBCTableSource.Builder builder = JDBCTableSource.builder()
                .setOptions(JDBCOptions.builder()
                        .setDBUrl(url)
                        .setDriverName(driver)
                        .setUsername(username)
                        .setPassword(password)
                        .setTableName(table)
                        .build())
                .setSchema(TableSchema.builder().fields(
                        fields.split(","),
                        converDataTypes(types))
                        .build());
        return builder;
    }

    public static DataType[] converDataTypes(String types) {

        String[] typesS = types.split(",");

        DataType[] dataType = new DataType[typesS.length];
        for (int i = 0; i < typesS.length; i++) {
            switch (typesS[i]) {
                case "string": {
                    dataType[i] = DataTypes.STRING();
                    break;
                }
                case "int": {
                    dataType[i] = DataTypes.INT();
                    break;
                }
                default: {
                    break;
                }
            }
        }

        return dataType;
    }

    public static void main(String[] args) {



    }
}
