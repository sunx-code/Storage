package com.sunx.storage.pool;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.sunx.storage.DBConfig;

/**
 *
 */
public class C3p0Pool extends AbastractDataSource{
    /**
     * @author Administrator
     */
    private static class SingletonClassStance{
        private static C3p0Pool instance = new C3p0Pool();
    }
    /**
     * @return
     */
    public static C3p0Pool me(){
        return SingletonClassStance.instance;
    }

    /**
     * @param config
     * @return
     */
    @Override
    public C3p0Pool build(DBConfig config) {
        try{
            ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass(config.getDriver());
            dataSource.setJdbcUrl(config.getUrl());
            dataSource.setUser(config.getUser());
            dataSource.setPassword(config.getPwd());
            dataSource.setInitialPoolSize(DEFAULT_INIT_SIZE);
            dataSource.setMinPoolSize(config.getInt("minPoolSize"));
            dataSource.setInitialPoolSize(config.getInt("initialPoolSize"));
            dataSource.setAcquireIncrement(config.getInt("acquireIncrement"));
            dataSource.setAcquireRetryAttempts(config.getInt("acquireRetryAttempts"));
            dataSource.setMaxIdleTime(config.getInt("maxIdleTime"));
            dataSource.setMaxPoolSize(config.getInt("maxPoolSize"));

            pool.put(config.getName(),dataSource);
        }catch (Exception e){
            e.printStackTrace();
        }
        return this;
    }
}
