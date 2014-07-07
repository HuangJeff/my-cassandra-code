package com.jdbc2;

import java.sql.*;
import java.util.*;

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

/**
 * working with cassandra version 0.6.0 beta
 * @author asus1
 *
 */
public class CassandraJdbc {
	
	public static final String UTF8 = "UTF8";
	
	public static void main(String[] args) {
		try {
//			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
//		    Connection con = DriverManager.getConnection("jdbc:cassandra://localhost:9170/Keyspace1");
			
			TTransport tr = new TSocket("localhost", 9160);
            TProtocol proto = new TBinaryProtocol(tr);
            Cassandra.Client client = new Cassandra.Client(proto);
            tr.open();
            
            String keyspace = "Keyspace1";
            String columnFamily = "Standard1";
            String keyUserID = "1";
            
            // insert data
            long timestamp = System.currentTimeMillis();
            
            ColumnPath colPathName = new ColumnPath(columnFamily);
            colPathName.setColumn("fullName".getBytes(UTF8));
            //與cassandra版本不合，所以有些Method會錯
//            client.insert(keyspace, keyUserID, colPathName, "Chris Goffinet"
//            		.getBytes(UTF8), timestamp, ConsistencyLevel.ONE);
            
//            ColumnPath colPathAge = new ColumnPath(columnFamily);
//            colPathAge.setColumn("age".getBytes(UTF8));
//
//            client.insert(keyspace, keyUserID, colPathAge, "24".getBytes(UTF8),
//                            timestamp, ConsistencyLevel.ONE);
//
//            // read single column
//            System.out.println("single column:");
//            Column col = client.get(keyspace, keyUserID, colPathName,
//                            ConsistencyLevel.ONE).getColumn();
//
//            System.out.println("column name: " + new String(col.name, UTF8));
//            System.out.println("column value: " + new String(col.value, UTF8));
//            System.out.println("column timestamp: " + new Date(col.timestamp));
//
//            // read entire row
//            SlicePredicate predicate = new SlicePredicate();
//            SliceRange sliceRange = new SliceRange();
//            sliceRange.setStart(new byte[0]);
//            sliceRange.setFinish(new byte[0]);
//            predicate.setSlice_range(sliceRange);
//
//            System.out.println("\nrow:");
//            ColumnParent parent = new ColumnParent(columnFamily);
//            List<ColumnOrSuperColumn> results = client.get_slice(keyspace,
//                            keyUserID, parent, predicate, ConsistencyLevel.ONE);
//            for (ColumnOrSuperColumn result : results) {
//                    Column column = result.column;
//                    System.out.println(new String(column.name, UTF8) + " -> "
//                                    + new String(column.value, UTF8));
//            }
            
            System.out.println("Linked...");
            
            tr.close();
			
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
