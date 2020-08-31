package com.jason.distributedlock;

import com.jason.pandaLock.core.exception.PandaLockException;
import com.jason.pandaLock.core.serverImpl.ZkPandaLock;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class LockTest {

	@Test
	public void test() throws InterruptedException, IOException, KeeperException {
		ZkPandaLock zkPandaLock = new ZkPandaLock();
		zkPandaLock.connectZooKeeper("127.0.0.1:2181");
		zkPandaLock.tryLock();

		Thread.sleep(3000);

		zkPandaLock.releaseLock();
	}

	public static void main(String[] args) throws IOException, InterruptedException, PandaLockException, KeeperException {

		for (int i = 0; i < 10; i++) {
            new Thread() {
                public void run() {
                    try {
                    	ZkPandaLock zkPandaLock = new ZkPandaLock();
                    	zkPandaLock.connectZooKeeper("127.0.0.1:2181");
                    	zkPandaLock.lock();
                    	System.out.println(Thread.currentThread().getName()+"在做事，做完就释放锁");
                    	Thread.sleep(1000);
                    	System.out.println(Thread.currentThread().getName()+"我做完事情了");
                    	zkPandaLock.releaseLock();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }
	}
}
