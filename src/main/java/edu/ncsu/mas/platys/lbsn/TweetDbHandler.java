package edu.ncsu.mas.platys.lbsn;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
}
