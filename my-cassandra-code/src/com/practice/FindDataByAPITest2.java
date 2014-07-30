/**
 * 
 */
package com.practice;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * 用來測試，API使用Thrift<br>
 * 1.新增大量資料<br>
 * 2.读取某条数据的单个字段(API)<br>
 * 3.读取整条数据(API)<br>
 * @author asus1
 */
public class FindDataByAPITest2 {
	/** 使用 Keyspace1 keyspace  */
	private final String KEYSPACE = "Keyspace1";
	
	/** 使用 Student columnfamily  */
	private final String columnfamily = "Student";
	
	
	public TTransport getTransport() throws Exception {
		String host = "127.0.0.1";
		host = "192.168.137.101";
		//包装好的socket
		TTransport tr = new TFramedTransport(new TSocket(host, 9160));
		return tr;
	}
	
	public void transMain() {
		TTransport tr = null;
		try {
			tr = this.getTransport();
			tr.open();
			if(!tr.isOpen())
			{
				System.out.println("failed to connect server!");
			}
			
			TProtocol proto = new TBinaryProtocol(tr);
			Cassandra.Client client = new Cassandra.Client(proto);
			
			long temp = System.currentTimeMillis();
			
			client.set_keyspace(KEYSPACE);	//使用DEMO keyspace
			ColumnParent parent = new ColumnParent(columnfamily);//column family
			
			//新增大量測試資料
			//this.createData(client, parent);
			
			//Create Sex 與 Age 兩個欄位
			//this.createSexColumn(client, parent);
			
			
			String key3 = "a1";//读取key为a1的那条记录
			//key3 = "jsmith";//讀取key為jsmith的那條記錄
			
			/*
			 * 读取某条数据的单个字段
			 */
			ColumnPath path = new ColumnPath("Student"); //设置读取Student的数据
			//設定要查詢的欄位Key值
			path.setColumn(toByteBuffer("id"));	//读取id
			try {
				//第一種方式(簡)
				System.out.println("读取某条数据的单个字段 ==> " + 
						toString(client.get(toByteBuffer(key3), path, ConsistencyLevel.ONE).column.value));
				//第二種方式(繁)
				ColumnOrSuperColumn cos = client.get(toByteBuffer(key3), path, ConsistencyLevel.ONE);
				System.out.println("读取某条数据的单个字段 ==> " + toString(cos.column.name) + " -> " + toString(cos.column.value));
			} catch(NotFoundException notFindE) {
				System.err.println("查無資料...");
			}
			
			/*
			 * 读取整条数据
			 */
			SlicePredicate predicate = new SlicePredicate();
			//										new SliceRange(start, finiash, reversed(倒轉), count)
			SliceRange sliceRange = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 10);
			predicate.setSlice_range(sliceRange);
			List<ColumnOrSuperColumn> results = 
				client.get_slice(toByteBuffer(key3), parent, predicate, ConsistencyLevel.ONE);
			System.out.println("读取整条数据，查得資料筆數：" + results.size() + " 筆。");
			for (ColumnOrSuperColumn result : results)
			{
				Column column = result.column;
				System.out.println(toString(column.name) + " -> " + toString(column.value));
			}
			
			long temp4 = System.currentTimeMillis();
			System.out.println("time: " + (temp4 - temp) + " ms");//输出耗费时间
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(tr != null)
				tr.close();
		}
	}
	
	/**
	 * 这里我们插入10万条数据到Student内
	 * 每条数据包括id和name
	 * @throws Exception
	 */
	public void createData(Cassandra.Client client, ColumnParent parent) throws Exception {
		long temp = System.currentTimeMillis();
		/*
		 * 这里我们插入10万条数据到Student内
		 * 每条数据包括id和name
		 */
		String key_user_id = "a";
		for(int i = 0;i < 100000;i++)
		{
			String k = key_user_id + i;//key
			long timestamp = System.currentTimeMillis();//时间戳
			
			Column idColumn = new Column(toByteBuffer("id"));//column name
			idColumn.setValue(toByteBuffer(i + ""));//column value
			idColumn.setTimestamp(timestamp);
			client.insert(
				toByteBuffer(k), 
				parent, 
				idColumn, 
				ConsistencyLevel.ONE);
			
			Column nameColumn = new Column(toByteBuffer("name"));
			nameColumn.setValue(toByteBuffer("student" + i));
			nameColumn.setTimestamp(timestamp);
			client.insert(
				toByteBuffer(k), 
				parent, 
				nameColumn, 
				ConsistencyLevel.ONE);
		}
		
		long temp2 = System.currentTimeMillis();
		//20140707  測得10萬筆>>Create Data time: 19695 ms
		//20140707 連到VM上>>Create Data time: 59903 ms
		System.out.println("Create Data time: " + (temp2 - temp) + " ms");	//Create 10萬筆資料所秏費時間
	}
	
	/**
	 * 針對10万条数据Student，新增Column:Sex(要用來測試 Secondary Index)
	 * 每条数据包括id和name
	 * @throws Exception
	 */
	public void createSexColumn(Cassandra.Client client, ColumnParent parent) throws Exception {
		long temp = System.currentTimeMillis();
		String sex = "M";
		String key_user_id = "a";
		for(int i = 0;i < 100000;i++)
		{
			String k = key_user_id + i;//key
			long timestamp = System.currentTimeMillis();//时间戳
			
			Column nameColumn = new Column(toByteBuffer("sex"));//column name
			if(i % 2 == 1) sex = "F";
			else sex = "M";
			nameColumn.setValue(toByteBuffer(sex));//column value
			nameColumn.setTimestamp(timestamp);
			client.insert(
				toByteBuffer(k), 
				parent, 
				nameColumn, 
				ConsistencyLevel.ONE);
		}
		
		long temp2 = System.currentTimeMillis();
		
		System.out.println("Create Column time: " + (temp2 - temp) + " ms");
	}
	
	/*
	 * 将String转换为bytebuffer，以便插入cassandra
	 */
	public static ByteBuffer toByteBuffer(String value) 
		throws UnsupportedEncodingException
	{
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}
    
	/*
	 * 将bytebuffer转换为String
	 */
	public static String toString(ByteBuffer buffer) 
		throws UnsupportedEncodingException
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
			FindDataByAPITest2 dataByApiTest = new FindDataByAPITest2();
			dataByApiTest.transMain();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
