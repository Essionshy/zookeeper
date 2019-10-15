package zookeeper;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * @author William
 *
 */
public class WatchMore {
	//常量
	private static final Logger logger = Logger.getLogger(WatchMore.class);
	private static final String CONNECTSTRING="192.168.1.158:2181";
	private static final int SESSION_TIMEOUT=500 * 1000;
	private static final String PATH="/tingyu";
	//实例变量
	private ZooKeeper zk=null;
	private String oldValue=null;
	/**
	 * 
	 * @return 封装方法获取Zookeeper实例
	 * @throws IOException
	 */
	public ZooKeeper startZK() throws IOException 
	{
		return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, event -> {});
	}
	
	/**
	 * 
	 * @param zk
	 * @param nodePath
	 * @param nodeValue
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void createZNode(String nodePath,String nodeValue) throws KeeperException, InterruptedException {
		
		zk.create(nodePath, nodeValue.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}
	/**
	 * 	
	 * @param zk
	 * @param nodePath
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String getZNode(String nodePath) throws KeeperException, InterruptedException {
		String result=null;
		byte [] resultValue=zk.getData(nodePath,event ->{
			try {
				triggerValue(nodePath);
			} catch (KeeperException |InterruptedException e) {
				e.printStackTrace();
			}	
		},new Stat());
		result=new String(resultValue);
		oldValue=result;
		return result;	
	}
	/**
	 * 
	 * @param nodePath
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private boolean triggerValue(String nodePath) throws KeeperException, InterruptedException {
		String result=null;
		byte [] resultValue=zk.getData(nodePath,event ->{
			try {
				triggerValue(nodePath);
			} catch (KeeperException |InterruptedException e) {
				e.printStackTrace();
			}	
		},new Stat());
		result=new String(resultValue);
		String newValue=result;
		if(oldValue.equals(newValue)) {
			logger.info("************Not changed************ ");
			return false;
		}else {
			logger.info("***********"
					+ "*oldValue:"+oldValue+"\t newValue:"+newValue);
			oldValue=newValue;
			return true;
		}
		
	}
	/**
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException
	{	
		logger.debug("*************start");
		WatchMore watchMore=new WatchMore();
		watchMore.setZk(watchMore.startZK());				
		if(watchMore.getZk().exists(PATH, false)==null) {
			watchMore.createZNode(PATH, "helloZo okeeper");
			String resultValue=watchMore.getZNode(PATH);
			logger.info("*******************resultValue:"+resultValue);		
		}else {
			logger.info("**********要创建的结点 名称已存在");
		}
		Thread.sleep(Long.MAX_VALUE);	
		logger.debug("*************end");			
	}
	//setter--->getter
	public ZooKeeper getZk() {
		return zk;
	}
	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}

}
