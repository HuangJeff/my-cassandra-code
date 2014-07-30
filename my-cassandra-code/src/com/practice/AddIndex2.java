/**
 * 
 */
package com.practice;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * 利用程式來將某欄位加入index
 * @author asus1
 * 
 */
public class AddIndex2 {
	private static final String CASSANDRA_HOST = "localhost";
	private static String KEYSPACE_NAME = "Keyspace1";
	private static String COLUMN_FAMILY_NAME = "Student";	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TTransport tr = null;
		Cassandra.Client client = null;
		String AGE = "age";
		try {
			try {
				//transport.open();
				tr = new TFramedTransport(new TSocket(CASSANDRA_HOST, 9160));
				tr.open();
			} catch (TTransportException ex) {
				ex.printStackTrace();
	            System.exit(1);
	        }
			TProtocol proto = new TBinaryProtocol(tr);
			client = new Cassandra.Client(proto);
			
			//KsDef : Describes a keyspace.
	        KsDef ksDef = new KsDef();
	        ksDef.name = KEYSPACE_NAME;
//	        ksDef.replication_factor = 1;
	        ksDef.strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
	        
	        //CfDef : Describes a column family
	        CfDef cfDef = new CfDef(KEYSPACE_NAME, COLUMN_FAMILY_NAME);
	        cfDef.comparator_type = "UTF8Type";
	        
//	        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap(FULL_NAME.getBytes()), "UTF8Type");
//	        cfDef.addToColumn_metadata(columnDef);
	        
	        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap(AGE.getBytes()), "UTF8Type");
	        columnDef1.index_type = IndexType.KEYS;
	        cfDef.addToColumn_metadata(columnDef1);
	        
	        //ksDef.cf_defs = Arrays.asList(cfDef);
	        
	        String result = client.system_update_column_family(cfDef);
	        System.out.println("system_update_column_family result : " + result);
	    } catch(Exception e) {
	    	e.printStackTrace();
	    } finally {
	        //transport.close();
	    	if(tr != null)
	    		tr.close();
	    }
	}
	
}
