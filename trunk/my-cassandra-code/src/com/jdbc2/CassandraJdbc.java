package com.jdbc2;

import java.util.*;
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
 * 透過JDBC方式取得Cassandra 資料庫系統(system)資訊
 * @author asus1
 * @url http://www.datastax.com/documentation/cql/3.0/cql/cql_using/use_query_system_c.html
 */
public class CassandraJdbc {
	
	public static final String UTF8 = "UTF8";
	
	public static void getSystemTableInfo(Connection conn, String tableName) throws Exception {
		PreparedStatement statement = null;
		List<String> columnList = new ArrayList<String>();
	    try {
	    	String schema_keyspaces = "SELECT * FROM system." + tableName;
		    statement = conn.prepareStatement(schema_keyspaces);
		    ResultSet rs = statement.executeQuery();
		    
		    ResultSetMetaData rsmd = rs.getMetaData();
		    //int columnCount = rsmd.getColumnCount();
		    for(int i = 1;i<= rsmd.getColumnCount();i++) {
		    	String columnName = rsmd.getColumnName(i);
		    	columnList.add(columnName);
		    }
		    for(String colName : columnList) {
	        	System.out.print(colName);
	        	System.out.print("\t");
	        }
		    System.out.println("\n-----\t-----\t-----\t-----");
		    while(rs.next()) {
		        //System.out.print(rs.getString(1) + "\t" +rs.getString(2) + "\n");
		        for(String colName : columnList) {
		        	System.out.print(rs.getString(colName));
		        	System.out.print("\t");
		        }
		        System.out.println();
		    }
	    } finally {
	    	statement.close();
	    }
	}
	
	public static void main(String[] args) {
		Connection conn = null;
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
		    conn = DriverManager.getConnection("jdbc:cassandra://localhost:9160/system");
			
		    //Table Name ： schema_keyspaces
//		    getSystemTableInfo(conn, "schema_keyspaces");
		    
		    //Table Name ： local
		    getSystemTableInfo(conn, "local");
		    
		  //Table Name ： peers
//		    getSystemTableInfo(conn, "peers");
		    
		  //Table Name ： schema_columns
//		    getSystemTableInfo(conn, "schema_columns");
		    
		  //Table Name ： schema_columnfamilies
//		    getSystemTableInfo(conn, "schema_columnfamilies");
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(conn != null) conn.close();
			} catch(SQLException sqle) {
				sqle.printStackTrace();
			}
		}
	}

}
