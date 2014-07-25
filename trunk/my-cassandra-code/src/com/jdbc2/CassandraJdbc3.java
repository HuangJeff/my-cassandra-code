package com.jdbc2;

import java.sql.*;

/**
 * 透過JDBC方式與Cassandrs相連
 * @author asus1
 *
 */
public class CassandraJdbc3 {
	static {
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
			System.out.println("Cassandra Driver Load Success...");
		} catch(ClassNotFoundException cnfe) {
			cnfe.printStackTrace();
		}
	}
	
	public static Connection getCassandraConn() throws SQLException {
		Connection conn = null;
		String connectionUrl = "";
		String keyspace = "Keyspace1";
		try {
			//localhost Cassandra
			connectionUrl = "jdbc:cassandra://localhost:9160/" + keyspace;
			//VM Cassandrs
//			connectionUrl = "jdbc:cassandra://192.168.137.101:9160/" + keyspace;
			
			conn = DriverManager.getConnection(connectionUrl);
			
			System.out.println("Get DB Connection Success...");
//		} catch(SQLNonTransientConnectionException nonFTE) {
//			System.err.println("SQLNonTransientConnectionException : " + nonFTE.getMessage());
//			System.err.println("Doesn't Found KEYSPACE[" + keyspace + "]. Please Created...");
		} catch(SQLException sqlE) {
			System.out.println("Get DB Connection Fail...");
			sqlE.printStackTrace();
			throw sqlE;
		}
		return conn;
	}
	
	/**
	 * 另一種取得Connection的方式<br>
	 * 直接先連IP，在判斷Keyspace存在否來決定是否Create Keyspace<br>
	 * @return COnnection
	 * @throws SQLException
	 */
	public static Connection getCassandraConn2() throws SQLException {
		Connection conn = null;
		Connection tmpSysConn = null;
		String connectionUrl = "";
		String keyspace = null;
		try {
			//localhost Cassandra
//			connectionUrl = "jdbc:cassandra://localhost:9160/";
			//VM Cassandrs
			connectionUrl = "jdbc:cassandra://192.168.137.101:9160/";
			
			//Keyspace 不知道那一個(or尚未建立)，先使其連結至system這個Keyspace，在來建立想要的Keyspace
			keyspace = "system";
			tmpSysConn = DriverManager.getConnection(connectionUrl + keyspace);
			
			System.out.println("Get DB Connection2 Success...");
			//檢查想要的Keyspace存在否？
			//在create keyspace時，指定的字串中大小寫會被轉置成小寫，因此若指定Connection時，有可能會無法建立之
			keyspace = "mytestb";
			//Keyspace不存在，新增之
			boolean keyspaceIsExistFlag = showKeyspaceAndExist(tmpSysConn, keyspace);
			System.out.println("keyspaceIsExistFlag : " + keyspaceIsExistFlag);
			if(!keyspaceIsExistFlag) {
				boolean createSuccssFlag = createKeyspace(tmpSysConn, keyspace);
				//create 失敗，該怎麼辦？
				if(createSuccssFlag) {
					
				}
			}
			//Keyspace存在，將Connection導過去
			conn = DriverManager.getConnection(connectionUrl + keyspace);
		} catch(SQLException sqle) {
			sqle.printStackTrace();
		} finally {
			try {
				if(tmpSysConn != null)
					tmpSysConn.close();
			} catch(SQLException sqle) {
				sqle.printStackTrace();
			}
		}
		return conn;
	}
	
	public static boolean showKeyspaceAndExist(Connection conn, String keyspaceStr) throws SQLException {
		try {
			boolean keyspaceIsExist = false;
			String query = "SELECT keyspace_name, strategy_class FROM system.schema_keyspaces;";
		    PreparedStatement statement = conn.prepareStatement(query);
		    
		    ResultSet rs = statement.executeQuery();
		    
		    while(rs.next()) {
		        System.out.print(rs.getString(1) + "\t" +rs.getString(2) + "\n");
		        
		        if(keyspaceStr.equalsIgnoreCase(rs.getString(1))) {
		        	keyspaceIsExist = true;
		        }
		    }
		    statement.close();
		    
		    return keyspaceIsExist;
		} catch(SQLException sqle) {
			sqle.printStackTrace();
			throw sqle;
		}
	}
	
	private static Boolean createKeyspace(Connection conn, String strKeyspace) {
		Boolean bResult = false;
		try {
			String query = "CREATE KEYSPACE " + strKeyspace + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
			System.out.println("Create Keyspace Query::\n" + query);
			PreparedStatement statement = conn.prepareStatement(query);
			bResult = statement.execute();
			statement.close();
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return bResult;
	}
	
	public static void createColumnFamily(Connection conn) throws SQLException {
		String data="CREATE columnfamily news (key int primary key, category text , linkcounts int ,url text)";
		Statement st = conn.createStatement();
		st.execute(data);
		System.out.println("createColumnFamily end.");
	}
	
	public static void dropColumnFamily(Connection conn, String name) throws SQLException {
		String data="drop columnfamily " + name + ";";
		Statement st = conn.createStatement();
		st.execute(data);
	}
	
	public static void pouplateData(Connection conn) throws SQLException {
		String data =
				"BEGIN BATCH \n"+
						"insert into news (key, category, linkcounts,url) values (5,'class',71,'news.com') \n"+ 
						"insert into news (key, category, linkcounts,url) values (6,'education',15,'tech.com') \n"+
						"insert into news (key, category, linkcounts,url) values (7,'technology',415,'ba.com') \n"+
						"insert into news (key, category, linkcounts,url) values (8,'travelling',45,'google.com/teravel') \n"+
				"APPLY BATCH;";
		Statement st = conn.createStatement();
		st.executeUpdate(data);
		System.out.println("insert Data end.");
	}
	
	public static void deleteData(Connection conn) throws SQLException {
		String data =
				"BEGIN BATCH \n"+
						"delete from  news where key='user5' \n"+
						"delete  category from  news where key='user2' \n"+
				"APPLY BATCH;";
		Statement st = conn.createStatement();
		st.executeUpdate(data);
	}
	
	public static void updateData(Connection conn) throws SQLException {
		String t = "update news set category='sports', linkcounts=1 where key=5";          
		Statement st = conn.createStatement();
		st.executeUpdate(t);
	}
	
	public static void getData(Connection conn) throws Exception {
		String t = "SELECT * FROM news";          
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(t);
		while(rs.next())
		{
			System.out.println(rs.getInt("key"));
			for(int j=1;j<rs.getMetaData().getColumnCount()+1;j++)
			{
				System.out.println(rs.getMetaData().getColumnName(j) + " : " +
						rs.getString(rs.getMetaData().getColumnName(j)));
			}
		}
	}
	
	//-----Table 系列-----
	public static void createTable(Connection conn) throws SQLException {
		String data= "CREATE Table myTestTable(user_id varchar," +
				                 "user_name varchar," +
				                 "password varchar," +
				                 "gender varchar," +
				                 "session_token varchar," +
				                 "birth_year bigint," +
				                 "PRIMARY KEY (user_id));";
		Statement st = conn.createStatement();
		st.execute(data);
		System.out.println("Table 建立完成");
	}
	
	public static void dropTable(Connection conn, String strTable) {
		Boolean bResult = false;
        try {
            String qry = "drop Table " + strTable + ";";
            Statement smt = conn.createStatement();
            bResult = smt.execute(qry);
            System.out.println("TABLE(column family) [" + strTable + "] is dropped。" + bResult);
        } catch(Exception e) {
            e.printStackTrace();
        }
	}
	
	/**
	 * 似乎  Table格式(mytesttable)無法用insert into來新增資料
	 * @param conn
	 */
	public static void insertIntoData(Connection conn) {
		int iResult=0;
		try {
//			String strQry = "INSERT INTO mytesttable (user_id, user_name, gender) VALUES ('010571', 'Shihwen', 'M')";//insert
//			String strQry = "CREATE INDEX idxName ON myUsersTable (user_name);";//insert
//			String strQry = "UPDATE myUsersTable SET user_name='shihwen' WHERE user_id='010571'";//update
//			String strQry = "delete from myUsersTable WHERE user_id='010571'";//delete
			
//			PreparedStatement statement = conn.prepareStatement(strQry);
//			iResult = statement.executeUpdate();
//			statement.close();
			
			String data =
					"BEGIN BATCH \n"+
							"insert into mytesttable (user_id, user_name, gender) values ('010571', 'John', 'M') \n"+ 
							"insert into mytesttable (user_id, user_name, gender) values ('010572', 'Shihwen', 'M') \n"+
							"insert into mytesttable (user_id, user_name, gender) values ('010573', 'Alin', 'F') \n"+
							"insert into mytesttable (user_id, user_name, gender) values ('010574', 'Bob', 'M') \n"+
					"APPLY BATCH;";
			Statement st = conn.createStatement();
			//iResult = st.executeUpdate(data);
			boolean flag = st.execute(data);
			st.close();
			System.out.println("共儲存成功 " + iResult + " 筆。" + flag);
		} catch (SQLException se) {
			se.printStackTrace();
		}
	}
	
	public static void selectData(Connection conn) {
		try {
			//Cassandra 不支持 select count(0) 這種語法，要改 select count(1) or select count(*) 這兩種語法
			String strSQL = "select count(1) from myUsersTable";
			PreparedStatement statement = conn.prepareStatement(strSQL);
			ResultSet rs = statement.executeQuery();
			while(rs.next()) {
				System.out.println("myUsersTable 共有 " + rs.getInt(1) + " 筆。"); 
			}
			
			strSQL = "select user_id, user_name, gender from myUsersTable";
			statement = conn.prepareStatement(strSQL);
			rs = statement.executeQuery();
			while(rs.next()) {
				System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3)); 
			}
			rs.close();
			statement.close();
		} catch(SQLException sqle) {
			sqle.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Connection jdbcConn = null;
		try {
			System.out.println("To Get DB Connection...");
			jdbcConn = getCassandraConn();
			
//			jdbcConn = getCassandraConn2();
			
			createColumnFamily(jdbcConn);
			
			//pouplateData(jdbcConn);
			
			//updateData(jdbcConn);
			
			//getData(jdbcConn);
			
			//---Table 系列---
			//測試建立Table
			//createTable(jdbcConn);
			
			//drop Table 會失敗
			//dropTable(jdbcConn, "myuserstable");
			
			//資料CUD會怎麼樣
			insertIntoData(jdbcConn);
			
			//資料R會怎麼樣
			//selectData(jdbcConn);
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(jdbcConn != null) {
					jdbcConn.close();
				}
			} catch(SQLException sqle) {
				sqle.printStackTrace();
			}
		}
	}
}
