package com.sunx.storage.test;

import com.sunx.storage.DBConfig;
import com.sunx.storage.DBFactory;
import com.sunx.storage.DBUtils;
import com.sunx.storage.annotation.Row;
import com.sunx.storage.annotation.Table;
import com.sunx.storage.pool.DuridPool;
import org.junit.Before;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 测试类
 *
 *
 */
public class Test {
    private DBFactory factory;

    @Before
    public void init(){
        //初始化数据库连接池
        DBConfig config = new DBConfig("conf/db.properties");
        DuridPool.me().build(config);
        //构建数据库操作类
        factory = DBFactory.me();
    }

    /**
     * 测试获取注解的内容
     */
    @org.junit.Test
    public void test(){
        User user = new User();
        user.setUserAge(12);
        user.setUserName("sunx");

        Table table = user.getClass().getAnnotation(Table.class);
        System.out.println(table.table());

        Field[] fields = user.getClass().getDeclaredFields();
        for(Field f : fields){
            Row row = f.getAnnotation(Row.class);
            if(row == null){
                System.out.println(f.getName());
            }else{
                System.out.println(row.field());
            }
        }
    }

    /**
     * 测试toMap方法是否可用
     */
    @org.junit.Test
    public void test1(){
        User user = new User();
        user.setUserAge(12);
        user.setUserName("sunx");

        Map<String,Object> data = DBUtils.toMap(user);
        for(Map.Entry<String,Object> entry : data.entrySet()){
            System.out.println(entry.getKey() +"\t" + entry.getValue());
        }
    }

    /**
     * 测试：获取给定一个实体bean获取表名称
     */
    @org.junit.Test
    public void test2(){
        System.out.println(DBUtils.table(new User()));
    }

    /**
     * 测试: 插入操作
     */
    @org.junit.Test
    public void test3(){
        //当个对象测试
        User user = new User();
        user.setUserName("sunx");
        user.setUserAge(25);

        factory.insert("localhost",user);

        //集合测试
        List<User> us = new ArrayList<User>();
        for(int i=0;i<100;i++){
            User u = new User();
            u.setUserAge(i);
            u.setUserName("name:" + i);

            us.add(u);
        }
        factory.insert("localhost",us);
    }

    @org.junit.Test
    public void test4(){
        //测试查询所有
        List<User> list = factory.select("localhost",User.class);
        System.out.println(list.size());

        //测试查询根据一定条件查询
        User user = new User();
        user.setUserName("sunx");
        user.setUserAge(25);

        List<User> rs = factory.select("localhost",user);
        System.out.println(rs.size());
    }
}
