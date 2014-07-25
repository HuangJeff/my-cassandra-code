/**
 * 
 */
package com.practice;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;

/**
 * 使用Thrift API 測試
 * @author asus1
 *
 */
public class FindDataByAPITest {
	private static final String CASSANDRA_HOST = "localhost";
	
	private static final String KEYSPACE = "Keyspace1";
	
	private static final String columnFamily = "Users";
	
	/**
	 * 尚未測試....
	 * 1.ColumnParent/Column().setName/Column.setValue  這樣填值是否正確OK.......OK
	 * 2.值是否能夠新增......K
	 * 3.client.insert的語法操作
	 * @throws Exception
	 */
	public static void testInsertingColumn() throws Exception {
		TTransport tr = new TFramedTransport(new TSocket(CASSANDRA_HOST, 9160));
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		
		String rowkeyId = "PI123";
		long timestamp = System.currentTimeMillis();
		
		client.set_keyspace(KEYSPACE);
		ColumnParent parent = new ColumnParent("Users");
		
		Column nameColumn = new Column().setName("age".getBytes());
		nameColumn.setValue("18".getBytes());
		nameColumn.setTimestamp(timestamp);
		
		client.insert(ByteBufferUtil.bytes(rowkeyId), parent, nameColumn, ConsistencyLevel.ONE);
		
		tr.close();
	}
	
	public static void testGetColumnValue() throws Exception {
		TTransport tr = new TFramedTransport(new TSocket(CASSANDRA_HOST, 9160));
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		
		client.set_keyspace(KEYSPACE);
		
//		ColumnPath columnPath = new ColumnPath("Users");
		
//		Column resultColumn = client.get(ByteBuffer.wrap("Jeff".getBytes("UTF-8")), 
//				columnPath, 
//				ConsistencyLevel.ONE).getColumn();
		
		//NotFoundException()
		String key_user_id = "1";
		// read single column
		/*columnPath.setColumn(ByteBuffer.wrap("name".getBytes("UTF-8")));
		
        System.out.println(client.get(ByteBuffer.wrap(key_user_id.getBytes("UTF-8")), columnPath, ConsistencyLevel.ONE));
		*/
		
		//ColumnParent parent = new ColumnParent("Users");
		ColumnParent parent = new ColumnParent(columnFamily);
		
		key_user_id = "4";
		
		// read entire row
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange(
        		ByteBuffer.wrap("".getBytes("UTF-8")), 
        		ByteBuffer.wrap("".getBytes("UTF-8")), false, 10);
        predicate.setSlice_range(sliceRange);
        
        String rowKey = "1234"; //Key值，依照這個key才能取得到相對應的資料集
        
        List<ColumnOrSuperColumn> results = client.get_slice(ByteBuffer.wrap(rowKey.getBytes("UTF-8")),
        		parent, predicate, ConsistencyLevel.ONE);
        System.out.println("Data size::" + results.size());
        for (ColumnOrSuperColumn result : results)
        {
            Column column = result.column;
            System.out.println(toString(column.name) + " -> " + toString(column.value));
        }
        
		//String resultStr = new String(resultColumn.getValue());
		
		//System.out.println(">> " + resultStr);
		
		tr.close();
	}
	
	public static String toString(ByteBuffer buffer) throws UnsupportedEncodingException
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, "UTF-8");
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// insert Data
			testInsertingColumn();
			
			//get Data
			//testGetColumnValue();
			
			System.out.println("Finish...");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
