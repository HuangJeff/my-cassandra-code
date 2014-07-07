package com.jdbc2;

import java.sql.*;
import java.util.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;

/**
 * working with cassandra version 1.0.0<br>
 * 使用Thrift API方式與Cassandra相連
 * @author asus1
 *
 */
public class CassandraJdbc2 {
	
	public static void main(String[] args) {
		try {
			//TTransport tr = new TFramedTransport(new TSocket("localhost", 9160));
			TTransport tr = new TFramedTransport(new TSocket("192.168.137.101", 9160));
			
			 tr.open();
			
	        TProtocol proto = new TBinaryProtocol(tr);
	        Cassandra.Client client = new Cassandra.Client(proto);
	        
	        String key_user_id = "1";
	        //設定keyspace
	        //client.set_keyspace("Keyspace1");
	        client.set_keyspace("system");
	        //練習從system中取得list of keyspace相關資訊
	        
	        
	        
	        // insert data
	        long timestamp = System.currentTimeMillis();
	        
//	        ColumnParent parent = new ColumnParent("Standard1");
//	        
//	        Column nameColumn = new Column(toByteBuffer("name"));
//	        nameColumn.setValue(toByteBuffer("Chris Goffinet"));
//	        nameColumn.setTimestamp(timestamp);
//	        client.insert(toByteBuffer(key_user_id), parent, nameColumn, ConsistencyLevel.ONE);
//	        
//	        Column ageColumn = new Column(toByteBuffer("age"));
//	        ageColumn.setValue(toByteBuffer("24"));
//	        ageColumn.setTimestamp(timestamp);
//	        client.insert(toByteBuffer(key_user_id), parent, ageColumn, ConsistencyLevel.ONE);
//
//	        ColumnPath path = new ColumnPath("Standard1");
//
//	        // read single column
//	        path.setColumn(toByteBuffer("name"));
//	        System.out.println(client.get(toByteBuffer(key_user_id), path, ConsistencyLevel.ONE));
//
//	        // read entire row
//	        SlicePredicate predicate = new SlicePredicate();
//	        SliceRange sliceRange = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 10);
//	        predicate.setSlice_range(sliceRange);
//	        
//	        List<ColumnOrSuperColumn> results = client.get_slice(toByteBuffer(key_user_id), parent, predicate, ConsistencyLevel.ONE);
//	        for (ColumnOrSuperColumn result : results)
//	        {
//	            Column column = result.column;
//	            System.out.println(toString(column.name) + " -> " + toString(column.value));
//	        }
	        
	        System.out.println("To Do End...");
	        
	        tr.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static ByteBuffer toByteBuffer(String value) throws UnsupportedEncodingException
    {
        return ByteBuffer.wrap(value.getBytes("UTF-8"));
    }
        
    public static String toString(ByteBuffer buffer) throws UnsupportedEncodingException
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, "UTF-8");
    }
	
}
