package iotgo.util;

public class CommonUtil {

    /**
     * 获取入参中的时间戳
     * @param args flinkJob入参
     * @return
     */
    public static long getArgsTimeStamp(String[] args){
        long timeStamp;
        if (args.length != 0) {
            timeStamp = Long.parseLong(args[0]);
        } else {
            timeStamp = 0L;
        }
        return timeStamp;
    }
}
