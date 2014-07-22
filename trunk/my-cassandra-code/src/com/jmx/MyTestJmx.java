/**
 * 
 */
package com.jmx;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;

import com.jmx.test1.Client.ClientListener;

/**
 * @author asus1
 *
 */
public class MyTestJmx {

	/**
	 * @param args
	 * @category
	 * 1.connector JMX service URL:service:jmx:rmi:///jndi/rmi://hostName:portNum/jmxrmi <br>
	 */
	public static void main(String[] args) {
		String hostName = "localhost";
		String portNum = "7199";
		JMXConnector jmxc = null;
		try {
			// Create an RMI connector client and
	        // connect it to the RMI connector server
	        //
	        echo("\nCreate an RMI connector client and " +
	             "connect it to the RMI connector server");
	        JMXServiceURL url =
	        		//new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi");
	        		new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi");
	        jmxc = JMXConnectorFactory.connect(url, null);
	        
	        // Create listener
	        //
	        ClientListener listener = new ClientListener();

	        // Get an MBeanServerConnection
	        //
	        echo("\nGet an MBeanServerConnection");
	        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
	        waitForEnterPressed();

	        // Get domains from MBeanServer
	        //
	        echo("\nDomains:");
	        String domains[] = mbsc.getDomains();
	        Arrays.sort(domains);
	        for (String domain : domains) {
	            echo("\tDomain = " + domain);
	        }
	        waitForEnterPressed();
	        
	        // Get MBeanServer's default domain
	        //
	        echo("\nMBeanServer default domain = " + mbsc.getDefaultDomain());

	        // Get MBean count
	        //
	        echo("\nMBean count = " + mbsc.getMBeanCount());

	        // Query MBean names
	        //
	        echo("\nQuery MBeanServer MBeans:");
	        
	        //Search All
	        Set<ObjectName> names = new TreeSet<ObjectName>(mbsc.queryNames(null, null));
	        for (ObjectName name : names) {
	            //echo("\tObjectName = " + name);
	            if(name.toString().startsWith("java.lang")) {
	            	//echo("\tObjectName = " + name);
	            	
	            	/*
	            	 * 可以對應著JConsole的參數看，因為attribute每個MBean不一定都有，都相同
	            	 */
//	            	Object o = mbsc.getAttribute(name, "ObjectName");
//	            	echo("o is " + o);
//	            	try {
//	            		Object o2 = mbsc.getAttribute(name, "LoadedClassCount");
//	            		echo("o2 is " + o2);
//	            	} catch(javax.management.AttributeNotFoundException anfe) {
//	            		System.err.println(anfe.getMessage());
//	            	}
	            } else if(name.toString().startsWith("org.apache.cassandra.db")) {
//	            	echo("\tObjectName = " + name);
	            	
//	            	try {
//	            		Object o = mbsc.getAttribute(name, "Commitlog");
//	            		echo("o is " + o);
//	            	} catch(javax.management.AttributeNotFoundException anfe) {
//	            		System.err.println(anfe.getMessage());
//	            	}
	            	
	            	String atStr = "org.apache.cassandra.db:type=ColumnFamilies,keyspace=Keyspace1,columnfamily=Student";
	            	
	            	if(name.toString().equals(atStr)) {
	            		echo("\tObjectName = " + name);
	            		
	            		MBeanInfo mbi = mbsc.getMBeanInfo(name);
		            	String className = mbi.getClassName();
		            	String description = mbi.getDescription();
		            	int hashCode = mbi.hashCode();
		            	
		            	echo("◎clzName=" + className + " description=" + description + " hCode=" + hashCode);
		            	
		            	MBeanAttributeInfo[] aryOfmbeanA =	mbi.getAttributes();
		            	for(MBeanAttributeInfo item : aryOfmbeanA) {
		            		echo("Name: " + item.getName() + " Type:" + item.getType());
		            		try {
			            		Object o3 = mbsc.getAttribute(name, item.getName());
			            		echo("◎" + o3);
		            		} catch(javax.management.AttributeNotFoundException anfe) {
			            		System.err.println("Exception：" + anfe.getMessage());
			            	}
		            	}
		            	
		            	echo("====================================");
		            	MBeanNotificationInfo[] aryOfmbeanN =	mbi.getNotifications();
		            	for(MBeanNotificationInfo item : aryOfmbeanN) {
		            		echo("Name: " + item.getName() + " Type:" + item.getNotifTypes());
		            	}
		            	echo("====================================");
		            	
	            	}
	            	
//	            	try {
//	            		Object o2 = mbsc.getAttribute(name, "LoadedClassCount");
//	            		System.out.println("o2 is " + o2);
//	            	} catch(javax.management.AttributeNotFoundException anfe) {
//	            		System.err.println(anfe.getMessage());
//	            	}
	            	
	            } else if(name.toString().startsWith("org.apache.cassandra.net")) {
            		echo("\tObjectName = " + name);
	            	
            		MBeanInfo mbi = mbsc.getMBeanInfo(name);
            		MBeanAttributeInfo[] aryOfmbeanA =	mbi.getAttributes();
	            	for(MBeanAttributeInfo item : aryOfmbeanA) {
	            		echo("Name: " + item.getName() + " Type:" + item.getType());
	            		try {
		            		Object o3 = mbsc.getAttribute(name, item.getName());
		            		echo("●" + o3);
	            		} catch(javax.management.AttributeNotFoundException anfe) {
		            		System.err.println("Exception：" + anfe.getMessage());
		            	}
	            	}
            	}
	            
	        }
	        
	        
	        waitForEnterPressed();
			
			
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(jmxc != null) {
					echo("\nClose the connection to the server");
					jmxc.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	private static void echo(String msg) {
        System.out.println(msg);
    }
	
	private static void waitForEnterPressed() {
        try {
            echo("\nPress <Enter> to continue...");
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
}
