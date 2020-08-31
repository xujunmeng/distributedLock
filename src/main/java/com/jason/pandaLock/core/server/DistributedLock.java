package com.jason.pandaLock.core.server;

import org.apache.zookeeper.KeeperException;

import com.jason.pandaLock.core.exception.PandaLockException;

/**
 * @author 作者 E-mail:ruanjianlxm@sina.com
 * @version 创建时间：2015年9月11日 下午6:27:55
 * 类说明     定义分布式锁需要的方法
 */
public abstract class DistributedLock {
	/**
	 * 释放锁
	 * @throws PandaLockException 
	 * @throws KeeperException 
	 * @throws InterruptedException 
	 */
	public abstract void releaseLock();
	/**
	 * 尝试获得锁，能获得就立马获得锁，如果不能获得就立马返回
	 * @throws PandaLockException 
	 * @throws ConnectException 
	 */
	public abstract boolean tryLock();

}
