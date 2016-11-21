package com.sunx.storage.inter;

import com.sunx.storage.DBConfig;

import java.sql.Connection;

/**
 *
 */
public interface IDataSource {
    /**
     * @param config
     * @return
     */
    public IDataSource build(DBConfig config);

    /**
     * @return
     */
    public Connection getConn(String dbPoolName);
}
