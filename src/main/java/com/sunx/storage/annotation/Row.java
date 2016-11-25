package com.sunx.storage.annotation;

import java.lang.annotation.*;

@Documented //文档
@Retention(RetentionPolicy.RUNTIME) //在运行时可以获取
@Target({ElementType.FIELD}) //作用到字段上
@Inherited //子类会继承
public @interface Row {

    /**
     * 对应的数据库中字段名称
     * @return
     */
    public String field();
}
