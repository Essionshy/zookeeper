package zookeeper;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * @author William
 *
 */
public class HelloZK {
	private static final Logger logger = Logger.getLogger(HelloZK.class);
	private static final String CONNECTSTRING="192.168.1.158:2181";
	private static final int SESSION_TIMEOUT=500 * 1000;
	private static final String PATH="/com.tingyu.oa.UserService";
	/**
	 * 
	 * @return 封装方法获取Zookeeper实例
	 * @throws IOException
	 */
	public ZooKeeper startZK() throws IOException 
	{
		return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
	}
	/**
	 * 停止zookeeper服务
	 * @param zk
	 * @throws InterruptedException
	 */
	public void stopZK(ZooKeeper zk) throws InterruptedException 
	{
		if (null!=zk) zk.close();
	}
	/**
	 * 
	 * @param zk
	 * @param nodePath
	 * @param nodeValue
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void createZNode(ZooKeeper zk,String nodePath,String nodeValue) throws KeeperException, InterruptedException {
		
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
	public String getZNode(ZooKeeper zk,String nodePath) throws KeeperException, InterruptedException {
		String result=null;
		byte [] resultValue=zk.getData(PATH, false, new Stat());
		result=new String(resultValue);
		return result;	
	}
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException
	{	
		logger.debug("*************start");
		HelloZK hello=new HelloZK();
		ZooKeeper zk=hello.startZK();
		if(zk.exists(PATH, false)==null) {
			hello.createZNode(zk, PATH, "helloZookeeper");
			String resultValue=hello.getZNode(zk, PATH);
			logger.info("*******************resultValue:"+resultValue);
		
		}else {
			logger.info("**********要创建的结点 名称已存在");
		}
		
		hello.stopZK(zk);
		logger.debug("*************end");
		
		
	}

}
