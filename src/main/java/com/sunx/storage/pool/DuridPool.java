package com.sunx.storage.pool;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.sunx.storage.DBConfig;
import com.sunx.storage.inter.IDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 */
public class DuridPool extends AbastractDataSource {
    /**
     * @author Administrator
     */
    private static class SingletonClassStance{
        private static DuridPool instance = new DuridPool();
    }
    /**
     * @return
     */
    public static DuridPool me(){
        return SingletonClassStance.instance;
    }

    /**
     * @param config
     * @return
     */
    @Override
    public IDataSource build(DBConfig config) {
        try{
            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setDriverClassName(config.getDriver());
            dataSource.setUrl(config.getUrl());
            dataSource.setUsername(config.getUser());
            dataSource.setPassword(config.getPwd());
            dataSource.setInitialSize(DEFAULT_INIT_SIZE);
            dataSource.setMinIdle(5);
            dataSource.setMaxActive(10);
            dataSource.setPoolPreparedStatements(false);

            pool.put(config.getName(),dataSource);
        }catch (Exception e){
            e.printStackTrace();
        }
        return this;
    }

    /**
     * @param connection
     */
    public void recycle(Connection connection){
        try {
            DruidPooledConnection conn = (DruidPooledConnection)connection;
            conn.recycle();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
