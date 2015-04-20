package edu.ncsu.mas.platys.lbsn.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TweetDbHandler implements AutoCloseable {

  private static final String dbConfigFileName = "dbconfig.properties";
  private static final String mysqlDriver = "com.mysql.jdbc.Driver";

  private final String mDbUrl;
  private final Connection mConn;

  public TweetDbHandler() throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, IOException, SQLException {
    Class.forName(mysqlDriver).newInstance();

    Properties config = new Properties();
    config.load(Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(dbConfigFileName));
    String url = config.getProperty("url");
    String username = config.getProperty("username");
    String password = config.getProperty("password");

    mDbUrl = url + "?user=" + username + "&password=" + password;
    mConn = DriverManager.getConnection(mDbUrl);
  }

  @Override
  public void close() throws SQLException {
    if (mConn != null) {
      mConn.close();
    }
  }
  
  public Connection getConnection() {
    return mConn;
  }
  
  public long getCount(String tableName) throws SQLException {
    long numRows = -1;
    try (Statement st = mConn.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from " + tableName + " use index(primary)")) {
      if (rs.next()) {
        numRows = rs.getInt(1);
      }
    }
    return numRows;
  }
}
