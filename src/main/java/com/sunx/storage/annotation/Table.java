package com.sunx.storage.annotation;

import java.lang.annotation.*;

/**
 * 构建一个注解类
 * 1 标记表属于哪个数据库
 * 2 标记表名称
 */
@Documented //文档
@Retention(RetentionPolicy.RUNTIME) //在运行时可以获取
@Target({ ElementType.TYPE, ElementType.METHOD}) //作用到类，方法，接口上等
@Inherited //子类会继承
public @interface Table {
    /**
     * 用于标记表
     * @return
     */
    public String table() default "";
}