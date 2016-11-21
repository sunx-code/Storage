package com.sunx.storage;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by sunxing on 15/12/11.
 */
public class DB {
    private String dbDriver;
    private String dbUrl;
    private String dbUser;
    private String dbPwd;

    public static DB me(){
        return new DB();
    }

    public Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName(dbDriver);
        Connection conn = DriverManager.getConnection(dbUrl,dbUser,dbPwd);
        return conn;
    }

    public String getDbDriver() {
        return dbDriver;
    }
    public DB setDbDriver(String dbDriver) {
        this.dbDriver = dbDriver;
        return this;
    }
    public String getDbUrl() {
        return dbUrl;
    }
    public DB setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
        return this;
    }
    public String getDbUser() {
        return dbUser;
    }
    public DB setDbUser(String dbUser) {
        this.dbUser = dbUser;
        return this;
    }
    public String getDbPwd() {
        return dbPwd;
    }
    public DB setDbPwd(String dbPwd) {
        this.dbPwd = dbPwd;
        return this;
    }
}