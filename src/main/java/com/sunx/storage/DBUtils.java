package com.sunx.storage;

import com.sunx.storage.annotation.Row;
import com.sunx.storage.annotation.Table;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.Map.Entry;

/**
 * 封装工具包
 *
 * 1 链接的关闭
 * 2 sql的拼接
 * 3 注解的反射获取
 *
 */
public class DBUtils {
	/**
	 * 关闭pstmt对象,以及rs结果集
	 * @param pstmt
	 * @param rs
	 */
	public static void toClose(PreparedStatement pstmt,ResultSet rs){
		toClose(pstmt,rs,null);
	}

	/**
	 * 关闭
	 * @param pstmt
	 * @param rs
	 * @param conn
	 */
	public static void toClose(PreparedStatement pstmt, ResultSet rs, Connection conn){
		try {
			if(pstmt != null){
				pstmt.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if(rs != null){
				rs.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if(conn != null){
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取实体对象中成员变量的名称和值
	 * 将名称和值封装到map集合中
	 *
	 * @param t
	 * @return
	 */
	public static <T> Map<String,Object> toMap(T t){
		Field[] fields = t.getClass().getDeclaredFields();
		Map<String,Object> cell = new HashMap<String,Object>();
		for(Field f : fields){
			f.setAccessible(true);
			try {
				String name = f.getName();
				Row row = f.getAnnotation(Row.class);
				if(row != null && row.field() != null && row.field().length() > 0){
					name = row.field();
				}
				Object bean = f.get(t);
				//过滤掉值为null的数据
//				if(bean == null)continue;
				cell.put(name, bean);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return cell;
	}

	/**
	 * 获取实体对象中成员变量的名称和值
	 * 将名称和值封装到map集合中
	 *
	 * @param t
	 * @return
	 */
	public static <T> Map<String,Field> toMapField(T t){
		if(t == null)return null;
		Field[] fields = t.getClass().getDeclaredFields();
		Map<String,Field> cell = new HashMap<String,Field>();
		for(Field f : fields){
			f.setAccessible(true);
			try {
				String name = f.getName();
				Row row = f.getAnnotation(Row.class);
				if(row != null && row.field() != null && row.field().length() > 0){
					name = row.field();
				}
				cell.put(name, f);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			}
		}
		return cell;
	}

	/**
	 * 获取实体对象中成员变量的名称和值
	 * 将名称和值封装到map集合中
	 *
	 * @param list
	 * @return
	 */
	public static <T> List<Map<String,Object>> toMap(List<T> list){
		List<Map<String,Object>> maps = new ArrayList<Map<String, Object>>();
		for(T t : list){
			maps.add(toMap(t));
		}
		return maps;
	}

	/**
	 * 给定一个对象,获取实体bean对应的数据库表名称
	 * @param t  		实体bean
	 * @param <T>
     * @return
     */
	public static <T> String table(T t){
		Table table = t.getClass().getAnnotation(Table.class);
		if(table != null && table.table() != null && table.table().length() > 0)
			return table.table();
		return t.getClass().getSimpleName().toLowerCase();
	}

	/**
	 * 给定一个map集合
	 * 进行拼接sql
	 *
	 * @return
	 */
	public static String toSQL(Map<String,Object> fields){
		StringBuffer key = new StringBuffer();
		StringBuffer value = new StringBuffer();
		key.append("(");
		value.append("(");
		int i=0;
		for(Entry<String,Object> entry : fields.entrySet()){
			key.append(entry.getKey());
			value.append("?");
			if(i + 1 < fields.size()){
				key.append(",");
				value.append(",");
			}
			i++;
		}
		key.append(")");
		value.append(")");
		return key.toString() + " values" + value.toString();
	}

	/**
	 * 向pstmt对象中添加批处理的数据对象值
	 * @param pstmt
	 * @param bean
	 */
	public static void toBatch(PreparedStatement pstmt,Map<String,Object> bean){
		try {
			int i = 1;
			for(Map.Entry<String,Object> entry : bean.entrySet()){
				pstmt.setObject(i, entry.getValue());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据不同的操作封装数据库请求sql
	 * @param keys
	 * @param values
	 * @return
	 */
	public static String toSQL(String keys[],Object[] values,OperatorType type){
		if(type == OperatorType.SELECT)return toSQL(keys,values," where "," and ");
		if(type == OperatorType.UPDATE)return toSQL(keys,values," set ",",");
		return null;
	}

	/**
	 * 根据给定的关键字以及条件配置sql语句
	 * @param strs
	 * @param objs
	 * @param condition
	 * @param split
	 * @return
	 */
	public static String toSQL(String[] strs,Object[] objs,String condition,String split){
		StringBuffer buff = new StringBuffer();
		if(strs == null || objs == null)return "";
		if(strs.length == 0 && objs.length == 0)return "";
		if(strs.length != objs.length)return "";
		buff.append(condition);
		for(int i=0;i<strs.length;i++){
			if(Validate.isNull(objs[i]))continue;
			if(objs[i] instanceof String){
				if(strs[i].contains(" ")){
					buff.append(strs[i] + " '" + objs[i] + "'");
				}else{
					buff.append(strs[i] + "='" + objs[i] + "'");
				}
			}else{
				buff.append(strs[i] + "=" + objs[i]);
			}
			if(i + 1 < strs.length){
				buff.append(split);
			}
		}
		return buff.toString();
	}
	/**
	 * 将从数据库中查询出来的结果封装到map集合中
	 * @param rs
	 * @return
	 */
	public static List<Map<String,Object>> toMap(ResultSet rs){
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		try {
//			遍历集合中的数据
			List<String> meta = new ArrayList<String>();
			for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
				meta.add(rs.getMetaData().getColumnName(i));
			}
			while(rs.next()){
				Map<String,Object> cell = new HashMap<String,Object>();
				for(int i=0;i<meta.size();i++){
					Object value = rs.getObject(meta.get(i));
					cell.put(meta.get(i), value);
				}
				list.add(cell);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 将查询到的结果集合封装到对象对应的字段上
	 * @param rs
	 * @param bean
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> List<T> toBean(List<Map<String,Object>> rs,Class bean){
		List<T> list = new ArrayList<T>();
		for(Map<String,Object> cell : rs){
//			通过class来构造一个实体类
			T obj = null;
			try {
				obj = (T)bean.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
//			获取实体bean的字段与数据库中字段对应的map集合
			Map<String,Field> relations = toMapField(obj);

//			给实体类的内一个属性赋值
			for(Entry<String,Object> entry : cell.entrySet()){
				try {
					Field field = relations.get(entry.getKey());
					if(field == null)throw new Exception("无法找到字段对应的关系:" + entry.getKey());
					field.setAccessible(true);
					setBean(field,obj,entry.getValue());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
//			将 赋值完成的一个实体添加到结果集合中
			if(obj != null){
				list.add(obj);
			}
		}
		return list;
	}
	/**
	 * 给对象的某一个属性赋值
	 * @param field
	 * @param obj
	 * @param value
	 */
	public static void setBean(Field field,Object obj,Object value){
		try {
			if(value == null)return;
			if(value instanceof java.sql.Timestamp){
				field.set(obj,value.toString());
			}else{
				field.set(obj, value);
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据给定的数组来拼接出字符串
	 * @param keys
	 * @param split
	 * @return
	 */
	public static String toSQL(String[] keys,String split){
		StringBuffer buff = new StringBuffer();
		try {
			for(int i=0;i<keys.length;i++){
				buff.append(keys[i]);
				if(i + 1 < keys.length){
					buff.append(split);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return buff.toString();
	}
}