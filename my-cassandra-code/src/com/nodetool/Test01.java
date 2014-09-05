/**
 * 
 */
package com.nodetool;

import org.apache.cassandra.tools.NodeCmd;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.Pair;


/**
 * 載入JAR<br>
 * apache-cassandra-1.1.5.jar<br>
 * commons-cli-1.2.jar<br>
 * snakeyaml-1.9.jar<br>
 * @author asus1
 * @see <a href="http://wiki.apache.org/cassandra/NodeTool">Cassandra Wiki Nodetool</a>
 * @see <a href="http://javadox.com/org.apache.cassandra/cassandra-all/2.0.4/overview-summary.html">API</a>
 */
public class Test01 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String host = "127.0.0.1";
			host = "192.168.137.101"; //vm 01 IP
			int port = 9160;
			port = 7199;
			
			NodeProbe nodeProbe = new NodeProbe(host, port);
			
			NodeCmd nodeCmd = new NodeCmd(nodeProbe);
			
//			new org.apache.cassandra.tools.NodeCmd(nodeprobe);
			
//			new org.apache.cassandra.tools.NodeProbe(host, port, username, password);
//			new org.apache.cassandra.tools.NodeProbe(host,port);
//			new org.apache.cassandra.tools.NodeProbe(host);
			
			nodeCmd.printClusterDescription(System.out, host);
			nodeCmd.printReleaseVersion(System.out);
			
//			NodeCmd.ToolCommandLine toolCmd = 
			
//			nodeCmd.printInfo(System.out, cmd);
			
			nodeCmd.printCompactionThroughput(System.out);
			
			//final Pair<String, String> RESOLVE_IP = Pair.create("r", "resolve-ip");
			//RESOLVE_IP.left.
			//boolean resolveIP = nodeCmd.h; //RESOLVE_IP.left	
			//nodeCmd.printRing(System.out, "keyspace", true);
			
			System.out.println("==============================");
			nodeCmd.printRing(System.out, "Keyspace1", false);
			
			System.out.println("==============================");
			
			/*
			System.out.println("==============================");
			boolean ignoreMode = false; //忽略模式
			//String[] s = {"Keyspace1", "system"}; //底下只會印Keyspace
			String[] s = {"Keyspace1"};
			nodeCmd.printColumnFamilyStats(System.out, ignoreMode, s);
			System.out.println("==============================");
			*/
			
			//args = new String[]{"VERSION", "TPSTATS"};
//			args = new String[]{"CFSTATS", "Keyspace1", "system"};
			
//			NodeCmd.main(args);
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
