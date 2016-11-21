package com.sunx.storage.pool;


import com.sunx.storage.DBConfig;
import com.sunx.storage.inter.IDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public abstract class AbastractDataSource implements IDataSource {
    public Map<String,DataSource> pool =  new ConcurrentHashMap<String, DataSource>();
    public ConcurrentHashMap<String,DBConfig> conf = new ConcurrentHashMap<String,DBConfig>();
    public int DEFAULT_INIT_SIZE = 5;

    /**
     * @param config
     * @return
     */
    public abstract IDataSource build(DBConfig config);

    /**
     * @param dbPoolName
     * @return
     */
    public synchronized Connection getConn(String dbPoolName) {
        if(!pool.containsKey(dbPoolName))return null;
        try{
            return pool.get(dbPoolName).getConnection();
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
