/**
 * 
 */
package com.practice;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * 利用程式來將某欄位加入index(網路上範例)
 * @author asus1
 * @see <a href="http://stackoverflow.com/questions/4677057/how-to-programatically-add-index-to-cassandra-0-7">網路範例</a>
 */
public class AddIndex {
	private static final String CASSANDRA_HOST = "localhost";
	private static String KEYSPACE_NAME = "";
	private static String COLUMN_FAMILY_NAME = "";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TTransport tr = null;
		Cassandra.Client client = null;
		
		String FULL_NAME = null;
		String BIRTH_DATE = null;
		String STATE = null;
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
	        
	        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap(FULL_NAME.getBytes()), "UTF8Type");
	        cfDef.addToColumn_metadata(columnDef);
	        
	        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap(BIRTH_DATE.getBytes()), "LongType");
	        columnDef1.index_type = IndexType.KEYS;
	        cfDef.addToColumn_metadata(columnDef1);
	        
	        ksDef.cf_defs = Arrays.asList(cfDef);
	        try {
	            client.system_add_keyspace(ksDef);
	            
	            client.set_keyspace(KEYSPACE_NAME);
	            
	            ColumnParent columnParent = new ColumnParent();
	            columnParent.column_family = COLUMN_FAMILY_NAME;
	            
	            //第一組資料(FULL_NAME)
	            //Column column = new Column(ByteBuffer.wrap(FULL_NAME.getBytes()),ByteBuffer.wrap("Brandon Sanderson".getBytes()), System.currentTimeMillis());
	            Column column = new Column();
	            client.insert(ByteBuffer.wrap("bsanderson".getBytes()), columnParent, column, ConsistencyLevel.ONE);
	            
	            //第二組資料(BIRTH_DATE)
	            column.name = ByteBuffer.wrap(BIRTH_DATE.getBytes());
	            column.value = ByteBuffer.allocate(8).putLong(1975);
	            client.insert(ByteBuffer.wrap("bsanderson".getBytes()), columnParent, column, ConsistencyLevel.ONE);
	            
	            //其它組資料(可以用column.name方式來指定，省去重覆NEW物件的程序)
	            column.name = ByteBuffer.wrap(FULL_NAME.getBytes());
	            column.value = ByteBuffer.wrap("Patrick Rothfuss".getBytes());
	            client.insert(ByteBuffer.wrap("prothfuss".getBytes()), columnParent, column, ConsistencyLevel.ONE);
	            column.name = ByteBuffer.wrap(BIRTH_DATE.getBytes());
	            column.value = ByteBuffer.allocate(8).putLong(1973);
	            client.insert(ByteBuffer.wrap("prothfuss".getBytes()), columnParent, column, ConsistencyLevel.ONE);

	            column.name = ByteBuffer.wrap(FULL_NAME.getBytes());
	            column.value = ByteBuffer.wrap("Howard Tayler".getBytes());
	            client.insert(ByteBuffer.wrap("htayler".getBytes()), columnParent, column, ConsistencyLevel.ONE);
	            column.name = ByteBuffer.wrap(BIRTH_DATE.getBytes());
	            column.value = ByteBuffer.allocate(8).putLong(1968);
	            client.insert(ByteBuffer.wrap("htayler".getBytes()), columnParent, column, ConsistencyLevel.ONE);
	            
	            column.name = ByteBuffer.wrap(STATE.getBytes());
	            column.value = ByteBuffer.wrap("WI".getBytes());
	            client.insert(ByteBuffer.wrap("prothfuss".getBytes()), columnParent, column, ConsistencyLevel.ONE);
	            column.value = ByteBuffer.wrap("UT".getBytes());
	            client.insert(ByteBuffer.wrap("htayler".getBytes()), columnParent, column, ConsistencyLevel.ONE);
	            
	            KsDef ks = client.describe_keyspace(KEYSPACE_NAME);
	            cfDef = new CfDef(ks.cf_defs.get(0));
	            ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap(STATE.getBytes()), "UTF8Type");
	            columnDef2.index_type = IndexType.KEYS;
	            cfDef.setColumn_metadata(Arrays.asList(columnDef, columnDef1, columnDef2));
	            
	            client.system_update_column_family(cfDef);
	            Thread.sleep(120000);//give cassandra enough time to build the index.
	            client.insert(ByteBuffer.wrap("bsanderson".getBytes()), columnParent, column, ConsistencyLevel.ONE);
	            
	            IndexClause indexClause = new IndexClause();
	            indexClause.start_key = ByteBuffer.allocate(0);
	            IndexExpression indexExpression = new IndexExpression();
	            indexExpression.column_name = ByteBuffer.wrap(STATE.getBytes());
	            indexExpression.value = ByteBuffer.wrap("UT".getBytes());
	            indexExpression.op = IndexOperator.EQ;
	            indexClause.addToExpressions(indexExpression);
	            
	            SliceRange sliceRange = new SliceRange();
	            sliceRange.count = 10;
	            sliceRange.start = ByteBuffer.allocate(0);
	            sliceRange.finish = ByteBuffer.allocate(0);
	            sliceRange.reversed = false;
	            SlicePredicate slicePredicate = new SlicePredicate();
	            slicePredicate.slice_range = sliceRange;
	            
	            List<KeySlice> keys = client.get_indexed_slices(columnParent, indexClause, slicePredicate, ConsistencyLevel.ONE);
	            if (!keys.isEmpty()) {
	                System.out.println("expecting: bsanderson htayler");
	                System.out.print("actual: ");
	                for (KeySlice key : keys) {
	                    System.out.print(new String(key.getKey()) + " ");
	                }
	            } else {
	                System.out.println("failed to find indexed item");
	            }
	        } catch (Exception ex) {
	            ex.printStackTrace();
	        } finally {
	            try {
	                client.system_drop_keyspace(KEYSPACE_NAME);
	            } catch (Exception ex) {
	                ex.printStackTrace();
	            }
	        }
	    } finally {
	        //transport.close();
	    	if(tr != null)
	    		tr.close();
	    }
	}
}
