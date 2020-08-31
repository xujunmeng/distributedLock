package com.jason.pandaLock.core.serverImpl;

import com.jason.pandaLock.core.exception.PandaLockException;
import com.jason.pandaLock.core.server.DistributedLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author 作者 E-mail:ruanjianlxm@sina.com
 * @version 创建时间：2015年10月15日 下午3:30:10 类说明 分布式锁的zk实现
 */
public class ZkPandaLock extends DistributedLock {

	/**
	 * pandaLock默认的EPHEMERAL节点的超时时间，单位毫秒
	 */
	private static final int DEFAULT_SESSION_TIMEOUT = 500000;
	/**
	 * 竞争者节点，每个想要尝试获得锁的节点都会获得一个竞争者节点
	 */
	private static final String COMPETITOR_NODE = "competitorNode";
	/**
	 * 统一的zooKeeper连接，在Init的时候初始化
	 */
	private static ZooKeeper pandaZk = null;

	private String lockName = null;

	private String rootPath = null;

	private String lockPath = null;

	private String competitorPath = null;

	private String thisCompetitorPath = null;

	@Override
	public void releaseLock()  {
		if (StringUtils.isBlank(rootPath) || StringUtils.isBlank(lockName)
				|| pandaZk == null) {
			throw new PandaLockException(
					"you can not release anyLock before you dit not initial connectZookeeper");
		}
		try {
			
		  pandaZk.delete(thisCompetitorPath, -1);
		  
		} catch (InterruptedException e) {
			throw new PandaLockException("the release lock has been Interrupted ");
		} catch (KeeperException e) {
			throw new PandaLockException("zookeeper connect error");
		}

	}

	@Override
	public boolean tryLock(){
		if (StringUtils.isBlank(rootPath) || StringUtils.isBlank(lockName)
				|| pandaZk == null) {
			throw new PandaLockException(
					"you can not tryLock anyone before you dit not initial connectZookeeper");
		}
		List<String> allCompetitorList = null;
		try {
			competitorPath = lockPath + "/" + COMPETITOR_NODE;

			//创建竞争者节点
			thisCompetitorPath = pandaZk.create(competitorPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

			allCompetitorList = pandaZk.getChildren(lockPath, false);

		} catch (KeeperException e) {
			throw new PandaLockException("zookeeper connect error");

		} catch (InterruptedException e) {
			e.printStackTrace();

		}
		Collections.sort(allCompetitorList);

		String thisCompetitorPathSubStr = thisCompetitorPath.substring((lockPath + "/").length());
		int index = allCompetitorList.indexOf(thisCompetitorPathSubStr);
		if (index == -1) {
			throw new PandaLockException("competitorPath not exit after create");

			// 如果发现自己就是最小节点,那么说明本人获得了锁
		} else if (index == 0) {
			return true;

			// 说明自己不是最小节点
		} else {
			return false;
		}
	}

	public void connectZooKeeper(String zkhosts)
			throws KeeperException, InterruptedException,
			IOException {
		if (StringUtils.isBlank(zkhosts)) {
			throw new PandaLockException("zookeeper hosts can not be blank");
		}
		if (StringUtils.isBlank(lockName)) {
			throw new PandaLockException("lockName can not be blank");
		}
		if (pandaZk == null) {
			pandaZk = new ZooKeeper(zkhosts, DEFAULT_SESSION_TIMEOUT,
					new Watcher() {

						@Override
						public void process(WatchedEvent event) {
							if (event.getState().equals(KeeperState.SyncConnected)) {
							}
						}
					});
		}
	}


}
