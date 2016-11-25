package com.sunx.storage.test;

import com.sunx.storage.annotation.Row;
import com.sunx.storage.annotation.Table;

/**
 * 用户实体类
 *
 * 1 使用注解,标志是哪个数据库
 * 2 使用注解,标志使用那张表
 *
 */
@Table(table = "test1")
public class User {
    /**
     * 用户名称
     */
    @Row(field = "uname")
    private String userName;

    /**
     * 用户密码
     */
    @Row(field = "age")
    private Integer userAge;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getUserAge() {
        return userAge;
    }

    public void setUserAge(Integer userAge) {
        this.userAge = userAge;
    }
}
