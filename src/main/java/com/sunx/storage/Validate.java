package com.sunx.storage;

import java.util.Collection;

public class Validate {
	/**
	 * 验证传递过来的对象是否为null
	 * @param bean
	 * @return
	 */
	public static boolean isNull(Object bean){
		if(bean == null)return judge(true,"the object is null....");
		return judge(false,"");
	}
	/**
	 *
	 * @param str
	 * @return true   字符串为空
	 * @return false  字符串非空
	 */
	public static boolean isStrEmpty(String str){
		if(!"".equals(str))return judge(false,"");
		return judge(true,"");
	}
	/**
	 * 判断给定的字符串是否为空或则是为null
	 * @param validate
	 * @return true  字符串为空或则是为null
	 * @return true  字符串为非空
	 */
	public static boolean isNullOrEmpty(String validate){
		return isNull(validate) || isStrEmpty(validate);
	}
	/**
	 * 判断是否为负数
	 * @param num
	 * @return
	 */
	public static boolean isNegative(int num){
		if(num <= 0)return judge(true,"");
		return judge(false,"");
	}
	/**
	 * 判断给定的一个数组是否为空
	 * @param bytes
	 * @return
	 */
	public static boolean isEmpty(Object[] bytes){
		if(bytes.length <= 0)return judge(true,"");
		return judge(false,"");
	}
	/**
	 * 判断给定的一个数组是否为空
	 * @param bytes
	 * @return
	 */
	public static boolean isEmpty(Collection<? extends Object> collection){
		if(collection.size() <= 0)return judge(true,"");
		return judge(false,"");
	}
	/**
	 * 判断给定的数组是否为空
	 * @param bytes
	 * @return
	 */
	public static boolean isNull(Object[] bytes){
		if(bytes == null)return judge(true,"");
		return judge(false,"");
	}
	/**
	 * 对代码重构
	 * @param flag
	 * @param msg
	 * @return
	 */
	private static boolean judge(boolean flag,String msg){
		try {
			if(!"".equals(msg)){
				throw new Exception(msg);
			}
		} catch (Exception e) {
		}
		return flag;
	}
}