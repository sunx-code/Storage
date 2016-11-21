package com.sunx.storage;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.Map.Entry;

/**
 * 数据库操作中的工具类,主要是一些操作的封装
 *
 *
 * @author Administrator
 */
@SuppressWarnings("rawtypes")
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
			Object obj = null;
			try {
				obj = bean.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
//			给实体类的内一个属性赋值
			for(Entry<String,Object> entry : cell.entrySet()){
				try {
					Field field = bean.getDeclaredField(entry.getKey());
					field.setAccessible(true);
					setBean(field,obj,entry.getValue());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
//			将 赋值完成的一个实体添加到结果集合中
			if(obj != null){
				list.add((T) obj);
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
	 * 获取一个对象对应的字段以及字段对应的属性值
	 * @param t
	 * @return
	 */
	public static <T> Map<String,Object> toMap(T t){
		Field[] fields = t.getClass().getDeclaredFields();
		Map<String,Object> cell = new HashMap<String,Object>();
		for(Field f : fields){
			f.setAccessible(true);
			try {
				cell.put(f.getName(), f.get(t));
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return cell;
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

	/**
	 * 更新操作sql
	 * @param keys
	 * @return
	 */
	public static String toSQL(String tableName,String[] keys,String[] conditions){
		StringBuffer buffer = new StringBuffer();
		try{
			buffer.append("update " + tableName + " set ");
			String key = toSQL(keys,",").replaceAll(","," = ?,").concat(" = ?");
			buffer.append(key);

			if(conditions.length > 0){
				String value = toSQL(conditions,",").replaceAll(","," =?  and ").concat(" = ?");
				buffer.append(" where ").append(value);
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		return buffer.toString();
	}

	/**
	 * @param bean
	 * @return
	 */
	public static String toSQL(Class bean){
		StringBuffer buff = new StringBuffer();
		Field[] fields = bean.getDeclaredFields();
		buff.append("(");
		int i = 0;
		for(Field field : fields){
			buff.append(field.getName());
			if(i + 1 < fields.length){
				buff.append(",");
			}
			i++;
		}
		buff.append(") values(");
		for(int j=0;j<i;j++){
			buff.append("?");
			if(j + 1 < i){
				buff.append(",");
			}
		}
		buff.append(")");

		return buff.toString();
	}
	/**
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
			if(entry.getValue() instanceof String){
				value.append("'" + entry.getValue() + "'");
			}else{
				value.append(entry.getValue());
			}
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
	 * @return
	 */
	public static String toSQL(String[]keys,Object[] values){
		StringBuffer buff = new StringBuffer();
		buff.append("(");
		for(int i=0;i<keys.length;i++){
			buff.append(keys[i]);
			if(i + 1 < keys.length){
				buff.append(",");
			}
		}
		buff.append(") values(");
		for(int j=0;j<values.length;j++){
//			插入数据库操作分为几类:1 字符串类型,2 数字类型,3 布尔类型,4 时间类型
			if(values[j] instanceof String){
				buff.append("'" + values[j] + "'");
			}else if(values[j] instanceof Boolean){
				buff.append((Boolean) values[j]?1:0);
			}else if(values[j] instanceof Date){
				buff.append("'" + values[j] + "'");
			}else{
				buff.append(values[j]);
			}
			if(j + 1 < values.length){
				buff.append(",");
			}
		}
		buff.append(")");
		return buff.toString();
	}
	/**
	 * 向pstmt对象中添加批处理的数据对象值
	 * @param pstmt
	 * @param bean
	 */
	public static <T> void toBatch(PreparedStatement pstmt,T bean){
		try {
			Field[] fields = bean.getClass().getDeclaredFields();
			int i = 1;
			for(Field field : fields){
				field.setAccessible(true);
				Object tmp = field.get(bean);
				pstmt.setObject(i, tmp);
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 向pstmt对象中添加批处理的数据对象值
	 * @param pstmt
	 */
	public static <T> void toBatch(PreparedStatement pstmt,List<Map<String,Object>> fields,String[] keys,Object[] conditionValues){
		try {
			for(int n = 0;n<fields.size();n++){
				Map<String, Object> field = fields.get(n);
				for(int i=0;i<keys.length;i++){
					Object value = field.get(keys[i]);
					pstmt.setObject(i + 1, value);
				}
				//设置条件
				if(conditionValues == null || conditionValues.length <= 0){
					pstmt.addBatch();
					continue;
				}
				//配置条件
				pstmt.setObject(keys.length + 1,conditionValues[n]);

				pstmt.addBatch();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 打印实体类的字段以及字段对应的值
	 * @param t
	 * @return
	 */
	public static <T> String toStr(T t){
		StringBuffer buff = new StringBuffer();
		buff.append("------------------\n");
		Map<String,Object> cell = toMap(t);
		for(Entry<String,Object> entry : cell.entrySet()){
			buff.append(entry.getKey() + ":" + entry.getValue() + "\n");
		}
		buff.append("------------------\n");
		return buff.toString();
	}
}