package edu.ncsu.mas.platys.lbsn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Table;

public class TweetWordDistributionBuilder {

  private static final Set<Long> userIds = new HashSet<Long>();
  private static final Map<String, Integer> tweetIdToGridIdMap = new HashMap<String, Integer>();
  
  private static final Map<Integer, Double> gridProbabDist = new HashMap<Integer, Double>();

  private static final Table<String, Integer, Integer> wordGridCountTable = HashBasedTable.create();
  private static final Map<String, Long> wordCountMap = new HashMap<String, Long>();

  public static void main(String[] args) throws FileNotFoundException, IOException,
      InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
    String inUserIdsFilename = args[0];
    String inGridFilename = args[1];
    String splitDate = args[2]; // YYYY-MM-DD
    
    String outGridDistFilename = args[3];

    TweetWordDistributionBuilder distBldr = new TweetWordDistributionBuilder();
    distBldr.train(inUserIdsFilename, inGridFilename, splitDate, outGridDistFilename);
    distBldr.test(splitDate);
  }
  
  public void train(String inUserIdsFilename, String inGridFilename, String splitDate,
      String outGridDistFilename) throws FileNotFoundException, InstantiationException,
      IllegalAccessException, ClassNotFoundException, IOException, SQLException {
    
    readUserIds(inUserIdsFilename);
    readTweetIdToGridId(inGridFilename);

    buildGridProbabilityDistribution(splitDate);
    writeGridProbabilityDistribution(outGridDistFilename);

    countWordsPerGrid(splitDate);
    countWords();
  }

  public void test(String splitDate) throws SQLException, InstantiationException,
      IllegalAccessException, ClassNotFoundException, IOException {
    try (TweetDbHandler dbHandler = new TweetDbHandler();
        Statement st = dbHandler.getConnection().createStatement();
        ResultSet rs = st.executeQuery("select t2.user_id, t2.tweet_id, t2.content "
            + "from twet_venue_new t1 tweet_topic t2"
            + "where creation_time >= '" + splitDate + "' ")) {
      while (rs.next()) {
        if (userIds.contains(rs.getLong(1))) {

        }
      }
    }
  }
  
  private void readUserIds(String inUserIdsFilename) throws FileNotFoundException, IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(inUserIdsFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: userId
        userIds.add(Long.parseLong(line.trim()));
      }
    }
    System.out.println("userIds size: " + userIds.size());
  }
    
  public Double findGridProbabilityGivenWords(Integer grid, String tweet) {
    String[] words = tweet.split("\\s+");
    Double probability = 0.0;
    for (String word : words) {
      probability *= wordGridCountTable.get(word, grid) / wordCountMap.get(word);
    }
    return probability * gridProbabDist.get(grid);
  }

  private void buildGridProbabilityDistribution(String splitDate) throws SQLException,
      InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
    Set<String> trainTweetIds = new HashSet<String>();
    try (TweetDbHandler dbHandler = new TweetDbHandler();
        Statement st = dbHandler.getConnection().createStatement();
        ResultSet rs = st.executeQuery("select user_id, tweet_id from twet_venue_new "
            + "where creation_time < '" + splitDate + "' ")) {
      while (rs.next()) {
        if (userIds.contains(rs.getLong(1))) {
          trainTweetIds.add(rs.getString(2));
        }
      }
    }

    SetMultimap<Integer, String> gridIdToTweetIdMap = HashMultimap.create();
    for (String tweetId : trainTweetIds) {
      gridIdToTweetIdMap.put(tweetIdToGridIdMap.get(tweetId), tweetId);
    }
    System.out.println("gridIdToTweetIdMap size: " + gridIdToTweetIdMap.size());

    for (Integer gridId : gridIdToTweetIdMap.keys()) {
      Set<String> tweetIds = gridIdToTweetIdMap.get(gridId);
      gridProbabDist.put(gridId, ((double) tweetIds.size()) / gridIdToTweetIdMap.size());
    }
    System.out.println("gridProbabDist size: " + gridProbabDist.size());
  }

  private void countWordsPerGrid(String splitDate) throws InstantiationException,
      IllegalAccessException, ClassNotFoundException, SQLException, IOException {
    try (TweetDbHandler dbHandler = new TweetDbHandler();
        Statement st = dbHandler.getConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      int pageSize = 10000;
      st.setFetchSize(pageSize);
      st.setMaxRows(pageSize);
      long numRows = dbHandler.getCount("tweet_topic");
      for (long pageNum = 0; pageNum <= numRows / pageSize; pageNum++) {
        ResultSet rs = st.executeQuery("select t1.tweet_id, t2.content "
            + "from tweet_topic t1 twet_venue_new t2 "
            + "where t2.creation_time < '" + splitDate + "' and t1.tweet_id = t2.tweet_id "
            + "limit " + (pageNum * pageSize) + ", " + pageSize);
        while (rs.next()) {
          String[] words = rs.getString(2).split("\\s+");
          for (String word : words) {
            Integer wordCount = wordGridCountTable.get(word, tweetIdToGridIdMap.get(rs.getInt(1)));
            if (wordCount == null) {
              wordCount = 1;
            } else {
              wordCount++;
            }
            wordGridCountTable.put(word, tweetIdToGridIdMap.get(rs.getInt(1)), wordCount);
          }
        }
      }
    }
    System.out.println("wordGridCountTable size: " + wordGridCountTable.size());
    System.out.println("wordGridCountTable rowKeySet size: "
        + wordGridCountTable.rowKeySet().size());
    System.out.println("wordGridCountTable columnKeySet size: "
        + wordGridCountTable.columnKeySet().size());
  }

  private void countWords() {
    for (String word : wordGridCountTable.rowKeySet()) {
      Collection<Integer> wordGridCounts = wordGridCountTable.row(word).values();
      Long count = 0L;
      for (Integer wordGridCount : wordGridCounts) {
        count += wordGridCount;
      }
      wordCountMap.put(word, count);
    }
    System.out.println("wordCountMap size: " + wordCountMap.size());
  }

  private void readTweetIdToGridId(String inGridFilename) throws FileNotFoundException, IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(inGridFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: tweetID,latitude,longitude,userID,gridID
        String[] lineParts = line.split(",");
        tweetIdToGridIdMap.put(lineParts[0], Integer.parseInt(lineParts[4]));
      }
    }
    System.out.println("tweetIdToGridIdMap size: " + tweetIdToGridIdMap.size());
  }

  private void writeGridProbabilityDistribution(String outGridDistFilename) throws IOException {
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(
        new FileWriter(outGridDistFilename)))) {
      for (Integer gridId : gridProbabDist.keySet()) {
        writer.println(gridId + "," + gridProbabDist.get(gridId));
      }
    }
  }
}
