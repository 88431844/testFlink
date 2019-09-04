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

    private Date createAt;

    private Date updateAt;
}
