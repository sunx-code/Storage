package com.sunx.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 主要对外提供一些简单的封装操作
 *
 * 对数据库的操作主要有4类:增删该查,以及以及一些表动作。
 *
 * 1 增加接口
 * 		//执行插入sql
 * 		boolean insert(pool,sql)
 *
 * 		//给定一个对象,将其插入到数据库中
 *		<T> boolean insert(pool,t)
 *
 *		//给定一个集合,将这个集合插入到数据库中
 *		<T> boolean insert(pool,List<T> lists)
 *
 *		//根据给定的map集合,将数据插入到数据库中
 *		boolean insert(pool,table,Map<String,Object> map)
 *
 *		//根据给定的list集合,将数据插入到数据库中
 *		boolean insert(pool,table,List<Map<String,Object>> list)
 *
 * 2 删除接口
 *
 *
 * 3 更新接口
 *
 *
 * 4 查询接口
 *
 *
 *
 * 5 表动作接口
 *
 *
 *
 * 6 sql执行接口
 *
 *
 * 7
 *
 */
public class DBFactory {
	//数据操作层
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

	/***************************插入操作*****************************/
	/**
	 * 根据给定的链接地址,将数据插入到数据库中
	 * @param pool  数据库连接池的名称
	 * @param sql	待执行sql
     * @return
     */
	public boolean insert(String pool,String sql){
		return dbHandler.insert(pool,sql);
	}

	/**
	 * 将数据库保存起来
	 * @param pool
	 * @param t
	 * @param <T>
     * @return
     */
	public <T> boolean insert(String pool,T t){
		if(Validate.isStrEmpty(pool) || Validate.isNull(t))return false;
		String table = DBUtils.table(t);
		Map<String,Object> map = DBUtils.toMap(t);
		return insert(pool,table,map);
	}

	/**
	 * 给定一个集合,将集合插入到数据库中
	 * @param pool 		数据库连接池名称
	 * @param list		数据集合
	 * @param <T>
     * @return
     */
	public <T> boolean insert(String pool, List<T> list){
		if(Validate.isStrEmpty(pool) || Validate.isNull(list) || Validate.isEmpty(list))return false;
		T t = list.get(0);
		String table = DBUtils.table(t);
		List<Map<String,Object>> maps = DBUtils.toMap(list);
		return insert(pool,table,maps);
	}

	/**
	 * 根据给定的一个集合,将数据插入到相应的集合中
	 * @param pool   		数据库连接池名称
	 * @param table			插入到那张表中
	 * @param map			待插入的map集合数据
     * @return
     */
	public boolean insert(String pool,String table,Map<String,Object> map){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table) || Validate.isNull(map) || Validate.isEmpty(map.keySet()))return false;
		List<Map<String,Object>> maps = new ArrayList<Map<String, Object>>();
		maps.add(map);
		return insert(pool,table,maps);
	}

	/**
	 * 根据给定的map集合,将数据插入到数据库中
	 * @param pool		数据库连接池名称
	 * @param table		待插入表名
	 * @param maps		待插入map数据集合
     * @return
     */
	public boolean insert(String pool,String table,List<Map<String,Object>> maps){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table) || Validate.isNull(maps) || Validate.isEmpty(maps))return false;
		Map<String,Object> node = maps.get(0);
		String sql = "insert into " + table + DBUtils.toSQL(node);
		return dbHandler.insert(pool, sql, maps);
	}

	/***************************删除操作*****************************/
	/**
	 * 根据给定的sql执行删除操作
	 * @param pool		数据库了连接池对应名称
	 * @param sql		待插入表名
     * @return
     */
	public boolean delete(String pool, String sql){
		return dbHandler.delete(pool,sql);
	}

	/**
	 * 通过给定的对象进行删除操作
	 * @param t 		待删除对象
	 * @param <T>
     * @return
     */
	public <T> boolean delete(String pool, T t){
		if(Validate.isNull(t))return false;
		String table = DBUtils.table(t);
		Map<String,Object> map = DBUtils.toMap(t);
		if(Validate.isNull(map) || Validate.isEmpty(map.keySet()))return false;
		return delete(pool, table, map.keySet().toArray(new String[map.size()]),map.values().toArray(new Object[map.size()]));
	}

	/**
	 * 通过给定相应的数据进行删除操作
	 * @param pool
	 * @param table
	 * @param keys
	 * @param values
	 */
	public boolean delete(String pool,String table,String[] keys,Object[] values){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return false;
		String sql = "delete " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return delete(pool,sql);
	}

	/***************************修改操作*****************************/
	/**
	 * @param pool
	 * @param sql
	 */
	public boolean update(String pool,String sql){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(sql))return false;
		return dbHandler.update(pool,sql);
	}
	/**
	 * 根据给定的条件更新数据库
	 * @param pool			数据库连接池对象名称
	 * @param table			待更新表名
	 * @param newkeys		需要更新的字段
	 * @param newvalues		待更新字段对应的值
	 * @param keys			条件字段
	 * @param values		条件字段对应的值
	 */
	public boolean update(String pool,String table,String[] newkeys,Object[] newvalues,String[] keys,Object[] values){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return false;
		if(Validate.isNull(newkeys) || Validate.isEmpty(newkeys))return false;
		if(Validate.isNull(newvalues) || Validate.isEmpty(newvalues))return false;
		String sql = "update " + table + DBUtils.toSQL(newkeys, newvalues, OperatorType.UPDATE) +
				DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return update(pool,sql);
	}
	/**
	 * 根据条件,将bean更新到数据库中
	 * @param pool 		数据库连接池
	 * @param t 		待更新对象
	 * @param keys   	更新条件的key
	 * @param values	更新条件的values
	 * @param <T>
	 */
	public <T> boolean update(String pool, T t, String[] keys, Object[] values){
		if(Validate.isNull(t))return false;
		String table = DBUtils.table(t);
		Map<String,Object> map = DBUtils.toMap(t);
		if(Validate.isNull(map) || Validate.isEmpty(map.keySet()))return false;
		return update(pool, table, map.keySet().toArray(new String[map.size()]),map.values().toArray(new Object[map.size()]), keys, values);
	}
	/***************************查询操作操作*****************************/
	/**
	 * 根据给定的sql语句,查询一个字符出来
	 * @param pool		数据库连接池对应的名称
	 * @param sql		指定的查询sql语句
	 * @param <T>
     * @return
     */
	public <T> List<T> find(String pool,String sql){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(sql))return null;
		return dbHandler.toList(pool,sql);
	}

	/**
	 * 根据给定的表,条件查询出相应的字段数据出来
	 * @param pool		数据库连接池对应的名称
	 * @param table		待查询表
	 * @param field		待查询字段
	 * @param keys		待查询条件字段
	 * @param values	待查询条件字段值
     * @param <T>		返回值类型
     * @return
     */
	public <T> List<T> find(String pool, String table, String field, String[] keys, Object[] values){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return null;
		String sql  = "select " + field + " from " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return find(pool,sql);
	}

	/**
	 * 根据给定的表,条件查询相应的数据出来
	 * @param pool		数据库连接池对应的名称
	 * @param table		待查询表
	 * @param field		待查询字段
	 * @param keys		待查询条件字段
	 * @param values	待查询字段对应的值
	 * @param topNum	查询多少天数据出来
     * @param <T>		返回值类型
     * @return
     */
	public <T> List<T> find(String pool,String table,String field,String[] keys,Object[] values,int topNum){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return null;
		String sql  = "select " + field + " from " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " limit " + topNum;
		return find(pool,sql);
	}

	/**
	 * 通过给定一个实体类的class对象,获取数据库中的数据
	 * @param pool		数据库连接池对应的名称
	 * @param clazz		待封装bean
	 * @param <T>
     * @return
     */
	public <T> List<T> select(String pool, Class clazz){
		if(Validate.isStrEmpty(pool) || Validate.isNull(clazz))return null;
		try{
			T t = (T)clazz.newInstance();
			String table = DBUtils.table(t);
			String sql = "select * from " + table;
			return dbHandler.toList(pool,sql,clazz);
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 根据给定的参数从数据库中获取指定行数的结果出来
	 * @param pool		数据库连接池对应的名称
	 * @param clazz		待封装bean
	 * @param topNum	指定查询多少行数据
	 * @param <T>
     * @return
     */
	public <T> List<T> select(String pool, Class clazz,int topNum){
		if(Validate.isStrEmpty(pool) || Validate.isNull(clazz))return null;
		try{
			T t = (T)clazz.newInstance();
			String table = DBUtils.table(t);
			String sql = "select * from " + table + " limit " + topNum;
			return dbHandler.toList(pool,sql,clazz);
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 根据指定的sql查询出数据后封装为指定bean
	 * @param pool		数据库连接池对应的名称
	 * @param sql		查询sql
	 * @param clazz		待封装bean
	 * @param <T>
     * @return
     */
	public <T> List<T> select(String pool, String sql, Class clazz){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(sql))return null;
		return dbHandler.toList(pool,sql,clazz);
	}

	/**
	 * 根据实体bean中不为空的字段的值进行查询
	 * @param pool	数据库连接池对应的名称
	 * @param t 	实体bean
	 * @param <T>
	 * @return
	 */
	public <T> List<T> select(String pool, T t){
		if(Validate.isStrEmpty(pool) || Validate.isNull(t))return null;
		String table = DBUtils.table(t);
		Map<String,Object> conditions = DBUtils.toMap(t);
		if(Validate.isNull(table) || Validate.isNull(conditions) || Validate.isEmpty(conditions.keySet()))return null;
		return select(pool,
				table,
				conditions.keySet().toArray(new String[conditions.size()]),
				conditions.values().toArray(new Object[conditions.size()]),
				t.getClass());
	}

	/**
	 * 根据实体bean中不为空的字段的值进行查询
	 * @param pool	数据库连接池对应的名称
	 * @param t 	实体bean
	 * @param <T>
	 * @return
	 */
	public <T> List<T> select(String pool, T t,int topNum){
		if(Validate.isStrEmpty(pool) || Validate.isNull(t))return null;
		String table = DBUtils.table(t);
		Map<String,Object> conditions = DBUtils.toMap(t);
		if(Validate.isNull(table) || Validate.isNull(conditions) || Validate.isEmpty(conditions.keySet()))return null;
		return select(pool,
				table,
				conditions.keySet().toArray(new String[conditions.size()]),
				conditions.values().toArray(new Object[conditions.size()]),
				topNum,
				t.getClass());
	}

	/**
	 * 根据指定条件查询出的结果封装到bean中
	 * @param pool		数据库连接池对应的名称
	 * @param table		待查询表
	 * @param keys		待查询条件字段
	 * @param values	待查询条件字段对应的值
	 * @param bean		结果封装bean的class对象
     * @param <T>
     * @return
     */
	public <T> List<T> select(String pool,String table, String[] keys,Object[] values, Class bean){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return null;
		String sql = "select * from " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return select(pool,sql,bean);
	}

	/**
	 * 根据指定条件查询出的结果封装到bean中
	 * @param pool		数据库连接池对应的名称
	 * @param table		待查询表
	 * @param keys		待查询条件字段
	 * @param values	待查询条件字段对应的值
	 * @param bean		结果封装bean的class对象
	 * @param topNum	查询结果行数
	 * @param <T>
	 * @return
	 */
	public <T> List<T> select(String pool,String table, String[] keys,Object[] values, int topNum, Class bean){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return null;
		if(Validate.isNegative(topNum))return null;
		String sql = "select * from " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " limit " + topNum;
		return select(pool,sql,bean);
	}

	/**
	 * 根据给定的条件和参数查询数据并封装到bean中
	 * @param pool		数据库连接池对应的名称
	 * @param table		待查询表
	 * @param keys		待查询条件字段
	 * @param values	待查询条件字段对应的值
	 * @param topNum	查询结果行数
	 * @param orderBy	排序方式
	 * @param bean		结果封装bean的class对象
	 * @param <T>
	 * @return
	 */
	public <T> List<T> select(String pool,String table,String[] keys,Object[] values,int topNum,String orderBy,Class bean){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return null;
		if(Validate.isNegative(topNum))return null;
		String sql = "select * from " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " " + orderBy + " limit " + topNum;
		return select(pool,sql,bean);
	}

	/**
	 * 根据条件查询出部分字段封装到bean中
	 * @param pool		数据库连接池对应的名称
	 * @param table		待查询表
	 * @param fields	待查询条件字段
	 * @param values	待查询条件字段对应的值
	 * @param bean		结果封装bean的class对象
	 * @return
	 */
	public <T> List<T> select(String pool,String table,String[] fields,String[] keys,Object[] values,Class bean){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return null;
		if(Validate.isNull(fields) || Validate.isEmpty(fields))return null;
		String sql = "select " + DBUtils.toSQL(fields, ",") + " from " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return select(pool,sql,bean);
	}
	/**
	 * 根据给定的条件查询指定行数的指定字段的内容数据封装到bean中
	 * @param pool		数据库连接池对应的名称
	 * @param table		待查询表
	 * @param fields	查询结果字段
	 * @param keys 		待查询条件字段
	 * @param values	待查询条件字段值
	 * @param topNum	查询条数
	 * @param bean		结果封装bean
	 * @return
	 */
	public <T> List<T> select(String pool,String table,String[] fields,String[] keys,Object[] values,int topNum,Class bean){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return null;
		if(Validate.isNull(fields) || Validate.isEmpty(fields))return null;
		if(Validate.isNegative(topNum))return null;
		String sql  = "select " + DBUtils.toSQL(fields, ",") + " from " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " limit " + topNum;
		return select(pool,sql,bean);
	}

	/**
	 * 通过sql查询结果封装到map中
	 * @param pool		数据库连接池对应的名称
	 * @param sql		待查询sql
	 * @return
	 */
	public List<Map<String,Object>> select(String pool,String sql){
		if(Validate.isStrEmpty(pool) || Validate.isNull(sql) || Validate.isStrEmpty(sql))return null;
		return dbHandler.toMap(pool,sql);
	}
	/**
	 * 通过特定条件查询出结果封装到map集合中
	 * @param pool		数据库连接池对应的名称
	 * @param table		待查询表
	 * @param keys		待查询条件字段
	 * @param values	待查询条件字段值
	 * @param topNum	查询条数
	 * @return
	 */
	public List<Map<String,Object>> select(String pool,String table,String[] keys,Object[] values,int topNum){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return null;
		if(Validate.isNegative(topNum))return null;
		String sql  = "select * from " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT) + " limit " + topNum;
		return dbHandler.toMap(pool,sql);
	}
	/**
	 * 根据条件查询书结果封装到map集合中
	 * @param pool		数据库连接池对应的名称
	 * @param table		待查询表
	 * @param keys		待查询条件字段
	 * @param values	待查询条件字段值
	 * @return
	 */
	public List<Map<String,Object>> select(String pool,String table,String[] keys,Object[] values){
		if(Validate.isStrEmpty(pool) || Validate.isStrEmpty(table))return null;
		String sql  = "select * from " + table + DBUtils.toSQL(keys, values, OperatorType.SELECT);
		return dbHandler.toMap(pool,sql);
	}

	/***************************表操作*****************************/

}