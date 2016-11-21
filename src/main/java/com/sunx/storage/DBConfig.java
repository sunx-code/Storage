package com.sunx.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class DBConfig {
    private String name;
    private String driver;
    private String url;
    private String user;
    private String pwd;
    private Properties properties = null;

    public DBConfig() {
    }

    public DBConfig(String path) {
        try {
            properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(new File(path))));
            init(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init(Properties p) {
        this.name = p.getProperty("name");
        this.driver = p.getProperty("driver");
        this.url = p.getProperty("url");
        this.user = p.getProperty("user");
        this.pwd = p.getProperty("pwd");
    }

    public String getName() {
        return name;
    }

    public DBConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getDriver() {
        return driver;
    }

    public DBConfig setDriver(String driver) {
        this.driver = driver;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public DBConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getUser() {
        return user;
    }

    public DBConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPwd() {
        return pwd;
    }

    public DBConfig setPwd(String pwd) {
        this.pwd = pwd;
        return this;
    }

    /**
     * @param key
     * @return
     */
    public String getString(String key){
        if(properties.containsKey(key))return properties.getProperty(key);
        return null;
    }

    /**
     * @param key
     * @return
     */
    public int getInt(String key){
        int result = -1;
        if(properties.containsKey(key)){
            result = Integer.parseInt(properties.getProperty(key));
        }
        return result;
    }

    /**
     * @param key
     * @return
     */
    public long getLong(String key){
        long result = -1;
        if(properties.containsKey(key)){
            result = Long.parseLong(properties.getProperty(key));
        }
        return result;
    }

    /**
     * @param key
     * @return
     */
    public boolean getBoolean(String key){
        boolean flag = false;
        if(properties.containsKey(key)){
            flag = Boolean.parseBoolean(properties.getProperty(key));
        }
        return flag;
    }
}
