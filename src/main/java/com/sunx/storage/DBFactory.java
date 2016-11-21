package com.sunx.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Administrator
 */
@SuppressWarnings("rawtypes")
public class DBFactory {
	private DBHandler dbHandler = null;
	/**
	 * @return
	 */
	public static DBFactory me(){
		return new DBFactory();
	}
	/**
	 */
	private DBFactory(){
		dbHandler = new DBHandler();
	}
	/**
	 * @param dbPoolName
	 * @param sql
	 */
	public void create(String dbPoolName,String sql){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(sql))return;
		dbHandler.create(dbPoolName,sql);
	}
	/**
	 * @param dbPoolName
	 * @param sql
	 */
	public List<Object> select(String dbPoolName,String sql){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(sql))return null;
		List<Object> list = dbHandler.select(dbPoolName,sql);
		return list;
	}
	/**
	 * @param dbPoolName
	 * @param sql
	 * @param bean
	 */
	public <T> List<T> select(String dbPoolName,String sql,Class bean){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(sql))return null;
		List<T> list = dbHandler.select(dbPoolName,sql,bean);
		return list;
	}
	/**
	 * values: '%www.taobao.com%'
	 * @param dbPoolName
	 * @param tableName
	 * @param values
	 * @param bean
	 */
	public <T> List<T> select(String dbPoolName,String tableName,String[] keys,Object[] values,Class bean){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		String sql = "select * from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return select(dbPoolName,sql,bean);
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param values
	 * @param topNum
	 * @param bean
	 * @return
	 */
	public <T> List<T> select(String dbPoolName,String tableName,String[] keys,Object[] values,int topNum,Class bean){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		if(Validate.isNegative(topNum))return null;
		String sql = "select * from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " limit " + topNum;
		return select(dbPoolName,sql,bean);
	}

	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param keys
	 * @param values
	 * @param topNum
	 * @param orderBy
	 * @param bean
     * @param <T>
     * @return
     */
	public <T> List<T> select(String dbPoolName,String tableName,String[] keys,Object[] values,int topNum,String orderBy,Class bean){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		if(Validate.isNegative(topNum))return null;
		String sql = "select * from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " " + orderBy + " limit " + topNum;
		return select(dbPoolName,sql,bean);
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param fields
	 * @param values
	 * @param bean
	 * @return
	 */
	public <T> List<T> select(String dbPoolName,String tableName,String[] fields,String[] keys,Object[] values,Class bean){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		if(Validate.isNull(fields) || Validate.isEmpty(fields))return null;
		String sql = "select " + DBUtils.toSQL(fields, ",") + " from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return select(dbPoolName,sql,bean);
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param fields
	 * @param values
	 * @param topNum
	 * @param bean
	 * @return
	 */
	public <T> List<T> select(String dbPoolName,String tableName,String[] fields,String[] keys,Object[] values,int topNum,Class bean){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		if(Validate.isNull(fields) || Validate.isEmpty(fields))return null;
		if(Validate.isNegative(topNum))return null;
		String sql  = "select " + DBUtils.toSQL(fields, ",") + " from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " limit " + topNum;
		return select(dbPoolName,sql,bean);
	}
	/**
	 * @param tableName
	 * @param values
	 * @param topNum
	 * @return
	 */
	public List<Map<String,Object>> select(String dbPoolName,String tableName,String[] keys,Object[] values,int topNum){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		if(Validate.isNegative(topNum))return null;
		String sql  = "select * from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " limit " + topNum;
		return dbHandler.select(dbPoolName,sql,0);
	}
	/**
	 * @return
	 */
	public List<Map<String,Object>> selectToMap(String dbPoolName,String SQL){
		if(Validate.isStrEmpty(dbPoolName))return null;
		return dbHandler.select(dbPoolName,SQL,0);
	}
	/**
	 * @param tableName
	 * @param values
	 * @return
	 */
	public List<Map<String,Object>> select(String dbPoolName,String tableName,String[] keys,Object[] values){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		String sql  = "select * from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return dbHandler.select(dbPoolName,sql,0);
	}

	/**
	 * @param tableName
	 * @param values
	 * @return
	 */
	public List<Map<String,Object>> select(String dbPoolName,String tableName,long limit,String[] keys,Object[] values){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		String sql  = "select * from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " limit " + limit;
		return dbHandler.select(dbPoolName,sql,0);
	}
	/**
	 * @param tableName
	 * @param field
	 * @param values
	 * @return
	 */
	public List<Object> select(String dbPoolName,String tableName,String field,String[] keys,Object[] values){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		String sql  = "select " + field + " from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return select(dbPoolName,sql,String.class);
	}
	/**
	 * @param tableName
	 * @param field
	 * @param values
	 * @param topNum
	 * @return
	 */
	public List<Object> select(String dbPoolName,String tableName,String field,String[] keys,Object[] values,int topNum){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return null;
		if(Validate.isNegative(topNum))return null;
		String sql  = "select " + field + " from " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " limit " + topNum;
		return select(dbPoolName,sql);
	}
	/**
	 * @param dbPoolName
	 * @param sql
	 */
	public boolean insert(String dbPoolName,String sql){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(sql))return false;
		return dbHandler.insert(dbPoolName,sql);
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param keys
	 * @param values
	 */
	public boolean insert(String dbPoolName,String tableName,String[] keys,Object[] values){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return false;
		String sql = "insert into " + tableName + DBUtils.toSQL(keys, values);
		return dbHandler.insert(dbPoolName,sql);
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param bean
	 */
	public <T> boolean insert(String dbPoolName,String tableName,T bean){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return false;
		if(Validate.isNull(bean))return false;
		List<T> list = new ArrayList<T>();
		list.add(bean);
		return insert(dbPoolName,tableName,list,bean.getClass());
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param beans
	 */
	public <T> boolean insert(String dbPoolName,String tableName,List<T> beans,Class cls){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return false;
		if(Validate.isNull(beans) || Validate.isEmpty(beans.toArray()))return false;
		String sql = "insert into " + tableName + DBUtils.toSQL(cls);
		return dbHandler.insert(dbPoolName, sql, beans);
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 */
	public boolean insert(String dbPoolName,String tableName,Map<String,Object> fields){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return false;
		if(Validate.isNull(fields) || Validate.isEmpty(fields.entrySet()))return false;
		String sql = "insert into " + tableName + DBUtils.toSQL(fields);
		return dbHandler.insert(dbPoolName, sql);
	}
	/**
	 * @param dbPoolName
	 * @param sql
	 */
	public void update(String dbPoolName,String sql){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(sql))return;
		dbHandler.update(dbPoolName,sql);
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param newkeys
	 * @param newvalues
	 * @param keys
	 * @param values
	 */
	public void update(String dbPoolName,String tableName,String[]newkeys,Object[]newvalues,String[] keys,Object[] values){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return;
		if(Validate.isNull(newkeys) || Validate.isEmpty(newkeys))return;
		if(Validate.isNull(newvalues) || Validate.isEmpty(newvalues))return;
		String sql = "update " + tableName + DBUtils.toSQL(newkeys, newvalues, OperatorType.UPDATE) +
				DBUtils.toSQL(keys, values, OperatorType.SELECT);
		dbHandler.update(dbPoolName,sql);
	}

	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param keys
     * @param values
     */
	public void update(String dbPoolName,String tableName,String[] keys,List<Map<String,Object>> values,String[] conditionKeys,Object[] conditionValues){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return;
		if(Validate.isNull(keys) || Validate.isEmpty(keys))return;
		if(Validate.isNull(values) || Validate.isEmpty(values))return;
		String sql = DBUtils.toSQL(tableName,keys,conditionKeys);
		dbHandler.update(dbPoolName,sql,keys,values,conditionValues);
	}

	/**
	 * @param dbPoolName
	 * @param tableName
	 * @param keys
	 * @param values
	 */
	public void delete(String dbPoolName,String tableName,String[] keys,Object[] values){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return;
		String sql = "delete " + tableName + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		dbHandler.delete(dbPoolName,sql);
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 */
	public void truncate(String dbPoolName,String tableName){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return;
		String sql = "truncate table " + tableName;
		dbHandler.truncate(dbPoolName, sql);
	}
	/**
	 * @param dbPoolName
	 * @param tableName
	 */
	public void drop(String dbPoolName,String tableName){
		if(Validate.isStrEmpty(dbPoolName) || Validate.isStrEmpty(tableName))return;
		String sql = "drop table " + tableName;
		dbHandler.drop(dbPoolName, sql);
	}
	/**
	 * @param dbPoolName
	 * @param execName
	 */
	public void exec(String dbPoolName,String execName){
		
	}
}