package com.sunx.storage;

import com.sunx.storage.pool.DuridPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 操作数据库
 * 1 数据连接池启动
 * 2 关闭数据库连接池
 * 3 重新连接数据库
 * 4 查询
 * 5 插入
 * 6 删除
 * 7 更新
 * 8 执行存储过程
 * 9 新建表
 *
 * @author Administrator
 */
@SuppressWarnings("rawtypes")
public class DBHandler {
	/**
	 * 创建表
	 * @param dbPoolName
	 * @param sql
	 */
	public void create(String dbPoolName,String sql){
		execute(dbPoolName,sql);
	}
	/**
	 * 通过给定的sql语句,执行获取结果集合,并将结果集合封装到对象中
	 * @param dbPoolName
	 * @param sql
	 * @return
	 */
	public List<Object> select(String dbPoolName,String sql){
//		获取数据库连接对象
		Connection conn = DuridPool.me().getConn(dbPoolName);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			if(conn == null){
				throw new Exception("get the connection error...");
			}
//			获取数据库连接正常,执行数据库操作
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();

			List<Object> list = new ArrayList<Object>();
			while(rs.next()){
				list.add(rs.getObject(1));
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DBUtils.toClose(pstmt, rs);
			DuridPool.me().recycle(conn);
		}
		return null;
	}
	/**
	 * 通过给定的sql语句,执行获取结果集合,并将结果集合封装到对象中
	 * @param dbPoolName
	 * @param sql
	 * @param type
	 * @return
	 */
	public List<Map<String,Object>> select(String dbPoolName,String sql,int type){
//		获取数据库连接对象
		Connection conn = DuridPool.me().getConn(dbPoolName);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			if(conn == null){
				throw new Exception("get the connection error...");
			}
//			获取数据库连接正常,执行数据库操作
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();

			List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
			while(rs.next()){
				Map<String,Object> temp = new HashMap<String,Object>();
				int column = rs.getMetaData().getColumnCount();
				for(int i=1;i<=column;i++){
					Object bean = rs.getObject(i);
					String name = rs.getMetaData().getColumnName(i);

					temp.put(name, bean);
				}

				list.add(temp);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DBUtils.toClose(pstmt, rs);
			DuridPool.me().recycle(conn);
		}
		return null;
	}
	/**
	 * 通过给定的sql语句,执行获取结果集合,并将结果集合封装到对象中
	 * @param dbPoolName
	 * @param sql
	 * @param bean
	 * @return
	 */
	public <T> List<T> select(String dbPoolName,String sql,Class bean){
//		获取数据库连接对象
		Connection conn = DuridPool.me().getConn(dbPoolName);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			if(conn == null){
				throw new Exception("get the connection error...");
			}
//			获取数据库连接正常,执行数据库操作
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();

//			将集合对象封装为map集合
			List<Map<String,Object>> list = DBUtils.toMap(rs);
//			对数据进行判断
			if(list == null || list.size() < 0){
				throw new Exception("将查询结果封装为list集合失败....");
			}
//			将集合封装到对象中
			List<T> temp = DBUtils.toBean(list, bean);
			return temp;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DBUtils.toClose(pstmt, rs);
			DuridPool.me().recycle(conn);
		}
		return null;
	}
	/**
	 * 执行插入操作
	 * @param dbPoolName
	 * @param sql
	 */
	public synchronized boolean insert(String dbPoolName,String sql){
		return execute(dbPoolName, sql);
	}
	/**
	 * 执行批量插入操作
	 * @param dbPoolName
	 * @param sql
	 */
	public <T> boolean insert(String dbPoolName,String sql,List<T> beans){
		boolean flag = false;
//		获取数据库连接对象
		Connection conn = DuridPool.me().getConn(dbPoolName);
		PreparedStatement pstmt = null;
		try {
			if(conn == null){
				throw new Exception("get the connection error...");
			}
//			获取数据库连接正常,执行数据库操作
			pstmt = conn.prepareStatement(sql);
//			关闭sql的自动提交功能
			conn.setAutoCommit(false);
			for(T t : beans){
				DBUtils.toBatch(pstmt,t);
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.commit();
			flag = true;
			System.out.println(Thread.currentThread().getName() + ",执行批量添加结束......");
		} catch (Exception e) {
		}finally{
			DBUtils.toClose(pstmt, null);
			DuridPool.me().recycle(conn);
		}
		return flag;
	}
	/**
	 * 给定sql执行更新操作
	 * @param dbPoolName
	 * @param sql
	 */
	public void update(String dbPoolName,String sql){
		execute(dbPoolName, sql);
	}

	/**
	 * 批量更更新操作
	 * @param dbPoolName
	 * @param sql
	 * @param keys
	 * @param values
	 * @param conditionValues
	 */
	public boolean update(String dbPoolName,String sql,String[] keys,List<Map<String,Object>> values,Object[] conditionValues){
		boolean flag = false;
//		获取数据库连接对象
		Connection conn = DuridPool.me().getConn(dbPoolName);
		PreparedStatement pstmt = null;
		try {
			if(conn == null){
				throw new Exception("get the connection error...");
			}
//			获取数据库连接正常,执行数据库操作
			pstmt = conn.prepareStatement(sql);
			conn.setAutoCommit(false);
			//添加批量操作的key
			DBUtils.toBatch(pstmt,values,keys,conditionValues);
			//执行批量操作
			pstmt.executeBatch();
			conn.commit();
			flag = true;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DBUtils.toClose(pstmt, null);
			DuridPool.me().recycle(conn);
		}
		return flag;
	}

	/**
	 * 给定sql执行删除操作
	 * @param dbPoolName
	 * @param sql
	 */
	public void delete(String dbPoolName,String sql){
		execute(dbPoolName, sql);
	}
	/**
	 * 清空表
	 * @param dbPoolName
	 * @param sql
	 */
	public void truncate(String dbPoolName,String sql){
		execute(dbPoolName, sql);
	}
	/**
	 * 删除表
	 * @param dbPoolName
	 * @param sql
	 */
	public void drop(String dbPoolName,String sql){
		execute(dbPoolName, sql);
	}
	/**
	 * 执行更新操作
	 * 更新操作包括： 插入,删除,更新
	 * @param dbPoolName
	 * @param sql
	 */
	private boolean execute(String dbPoolName,String sql){
		boolean flag = false;
//		获取数据库连接对象
		Connection conn = DuridPool.me().getConn(dbPoolName);
		PreparedStatement pstmt = null;
		try {
			if(conn == null){
				throw new Exception("get the connection error...");
			}
//			获取数据库连接正常,执行数据库操作
			pstmt = conn.prepareStatement(sql);
			conn.setAutoCommit(false);
			pstmt.executeUpdate();
			conn.commit();
			flag = true;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DBUtils.toClose(pstmt, null);
			DuridPool.me().recycle(conn);
		}
		return flag;
	}
}