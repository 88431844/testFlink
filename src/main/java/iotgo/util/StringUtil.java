package iotgo.util;

import org.apache.commons.lang3.StringUtils;

public class StringUtil {

    public static boolean isEmpty(String str){
        return StringUtils.isEmpty(str) || "null".equals(str);
    }

    public static boolean isNotEmpty(String str){
        return !isEmpty(str);
    }

    public static void main(String[] args) {
        System.out.println(StringUtil.isEmpty("null"));
    }
}
