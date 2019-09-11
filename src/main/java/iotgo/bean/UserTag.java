package iotgo.bean;

import lombok.*;

import java.util.Date;

@Data
@Builder
public class UserTag {

    private String uuid;

    private String tagName;

    private String tagDesc;

    private String tagType;

    private long createAt;

    private long updateAt;
    /**
     * 是否拥有该标签
     */
    private boolean haveTag;
}
