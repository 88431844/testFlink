package iotgo.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserTouchInfo {

    private String userId;
    private long eventTime;
    private String eventType;
    private String channel;
    private String uuid;
    private String openId;
    private long flowId;
    private long nodeTypeId;
    private String kafkaTopic;

    public static String convertToCsv(UserTouchInfo userTouchInfo) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");

        builder.append("'");
        builder.append(userTouchInfo.getUserId());
        builder.append("', ");

        builder.append(userTouchInfo.getEventTime());
        builder.append(", ");

        builder.append("'");
        builder.append(userTouchInfo.getEventType());
        builder.append("', ");

        builder.append("'");
        builder.append(userTouchInfo.getChannel());
        builder.append("', ");

        builder.append("'");
        builder.append(userTouchInfo.getUuid());
        builder.append("', ");

        builder.append("'");
        builder.append(userTouchInfo.getOpenId());
        builder.append("', ");

        builder.append(userTouchInfo.getFlowId());
        builder.append(", ");

        builder.append(userTouchInfo.getNodeTypeId());
        builder.append(", ");

        builder.append("'");
        builder.append(userTouchInfo.getKafkaTopic());
        builder.append("'");
        builder.append(" )");
        return builder.toString();
    }

}
