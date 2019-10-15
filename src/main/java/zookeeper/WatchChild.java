package zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

/**
 * 1.注册父节点并监控其子节点的变化
 * @author William
 *
 */
public class WatchChild {
	//常量
	private static final Logger logger = Logger.getLogger(WatchChild.class);
	private static final String CONNECTSTRING="192.168.1.158:2181";
	private static final int SESSION_TIMEOUT=500 * 1000;
	private static final String PATH="/tingyu";
	//实例变量
	private ZooKeeper zk=null;

	/**
	 * 
	 * @return 封装方法获取Zookeeper实例
	 * @throws IOException
	 */
	public ZooKeeper startZK() throws IOException 
	{
		return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, event -> {
			if(event.getType()==EventType.NodeChildrenChanged && event.getPath().equals(PATH))
			{
				showChildNode(PATH);
			}else {				
				showChildNode(PATH);//注册父节点并显示其所有子节点
			}				
		});
	}
	/**
	 * 显示父节点下的所有子节点
	 * @param nodePath
	 */
	public void showChildNode(String nodePath) 
	{
		List<String> list=null;
		try {
			list=zk.getChildren(nodePath, true);
			logger.info("*******************All children node:"+list);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
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
		WatchChild watchChild=new WatchChild();
		watchChild.setZk(watchChild.startZK());				
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
