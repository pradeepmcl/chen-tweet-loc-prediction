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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Table;

import edu.ncsu.mas.platys.lbsn.db.TweetDbHandler;

public class TweetLocationLearner2 {

  private final Set<Long> userIds;
  private final Map<String, Integer> tweetIdToGridIdMap;
  private final Map<Integer, Integer> gridIdToNeighborhoodMap;
  
  private final Map<Integer, Double> gridProbDist = new HashMap<Integer, Double>();
  private final Map<Integer, Double> neighborhoodProbDist = new HashMap<Integer, Double>();

  private final Table<String, Integer, Integer> wordGridCountTable = HashBasedTable.create();
  private final Map<String, Long> wordCountMap = new HashMap<String, Long>();
  
  private final Table<String, Integer, Double> wordGridProbDist = HashBasedTable.create();
  private final Table<String, Integer, Double> wordNeighborhoodProbDist = HashBasedTable.create();
  
  public TweetLocationLearner2(Set<Long> userIds, Map<String, Integer> tweetIdToGridIdMap,
      Map<Integer, Integer> gridIdToNeighborhoodMap) {
    this.userIds = userIds;
    this.tweetIdToGridIdMap = tweetIdToGridIdMap;
    this.gridIdToNeighborhoodMap = gridIdToNeighborhoodMap;
  }
  
  public void train(String splitDate, String outGridDistFilename,
      String outNeighborhoodDistFilename, String outWordGridDistFilename,
      String outWordNeighborhoodDistFilename) throws FileNotFoundException, InstantiationException,
      IllegalAccessException, ClassNotFoundException, IOException, SQLException {

    buildNeighborhoodAndGridProbabilityDistribution(splitDate);
    writeGridProbabilityDistribution(outGridDistFilename);
    writeNeighborhoodProbabilityDistribution(outNeighborhoodDistFilename);

    countWordsPerGrid(splitDate);
    countAndDiscardInfrequentWords(30);
    buildWordAndGridProbabilityDistribution();
    buildWordAndNieghborhoodProbabilityDistribution();
    cleanUp(); // clear unwanted (and space consuming) objects
    writeWordGridProbilityDistribution(outWordGridDistFilename);
    writeNeighborhoodProbilityDistribution(outWordNeighborhoodDistFilename);
  }

  public Map<Integer, Double> getGridProbabDist() {
    return Collections.unmodifiableMap(gridProbDist);
  }

  public Table<String, Integer, Double> getWordGridProbDist() {
    return wordGridProbDist; // TODO: return unmodifiable Table
  }

  private void cleanUp() {
    wordGridCountTable.clear();
    wordCountMap.clear();
  }
  
  private void buildNeighborhoodAndGridProbabilityDistribution(String splitDate) throws SQLException,
      InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
    Set<String> trainTweetIds = new HashSet<String>();
    try (TweetDbHandler dbHandler = new TweetDbHandler();
        Statement st = dbHandler.getConnection().createStatement();
        ResultSet rs = st.executeQuery("select user_id, tweet_id from tweet_venue_new "
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
      gridProbDist.put(gridId, ((double) tweetIds.size()) / gridIdToTweetIdMap.size());
    }
    System.out.println("gridProbabDist size: " + gridProbDist.size());
    
    SetMultimap<Integer, Integer> neighborhoodIdToGridIdMap = HashMultimap.create();
    for (Integer gridId : gridIdToNeighborhoodMap.keySet()) {
      neighborhoodIdToGridIdMap.put(gridIdToNeighborhoodMap.get(gridId), gridId);
    }
    System.out.println("gridIdToNeighborhoodMap size: " + gridIdToNeighborhoodMap.size());
    
    for (Integer neighborhoodId : neighborhoodIdToGridIdMap.keys()) {
      Set<Integer> gridIds = neighborhoodIdToGridIdMap.get(neighborhoodId);
      double tweetCount = 0.0;
      for (Integer gridId : gridIds) {
        tweetCount += gridIdToTweetIdMap.get(gridId).size();
      }
      neighborhoodProbDist.put(neighborhoodId, tweetCount / gridIdToTweetIdMap.size());
    }
    System.out.println("neighborhoodProbDist size: " + neighborhoodProbDist.size());
  }

  private void writeGridProbabilityDistribution(String outGridDistFilename) throws IOException {
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(
        new FileWriter(outGridDistFilename)))) {
      for (Integer gridId : gridProbDist.keySet()) {
        writer.println(gridId + "," + gridProbDist.get(gridId));
      }
    }
  }
  
  private void writeNeighborhoodProbabilityDistribution(String outNeighborhoodDistFilename)
      throws IOException {
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
        outNeighborhoodDistFilename)))) {
      for (Integer neighborhoodId : neighborhoodProbDist.keySet()) {
        writer.println(neighborhoodId + "," + neighborhoodProbDist.get(neighborhoodId));
      }
    }
  }
  
  private void countWordsPerGrid(String splitDate) throws InstantiationException,
      IllegalAccessException, ClassNotFoundException, SQLException, IOException {
    try (TweetDbHandler dbHandler = new TweetDbHandler();
        Statement st = dbHandler.getConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      int pageSize = 10000;
      st.setFetchSize(pageSize);
      st.setMaxRows(pageSize);
      for (long pageNum = 0; true; pageNum++) {
        try (ResultSet rs = st.executeQuery("select t2.user_id, t1.tweet_id, t1.content "
            + "from tweet_topic t1, tweet_venue_new t2 "
            + "where t2.creation_time < '" + splitDate + "' and t1.tweet_id = t2.tweet_id " 
            + "limit " + (pageNum * pageSize) + ", " + pageSize)) {
          if (!rs.next()) {
            break;
          }
          do {
            long userId = rs.getLong(1);
            if (userIds.contains(userId)) {
              String tweetId = rs.getString(2);
              String[] words = rs.getString(3).split("\\s+");
              for (String word : words) {
                Integer wordCount = wordGridCountTable.get(word,
                    tweetIdToGridIdMap.get(tweetId));
                if (wordCount == null) {
                  wordCount = 1;
                } else {
                  wordCount++;
                }
                wordGridCountTable.put(word, tweetIdToGridIdMap.get(tweetId), wordCount);
              }
            }
          } while (rs.next());
        }
      }
    }
    System.out.println("wordGridCountTable size: " + wordGridCountTable.size());
    System.out.println("wordGridCountTable rowKeySet size: "
        + wordGridCountTable.rowKeySet().size());
    System.out.println("wordGridCountTable columnKeySet size: "
        + wordGridCountTable.columnKeySet().size());
  }

  private void countAndDiscardInfrequentWords(int minCount) {
    Iterator<String> wordItr = wordGridCountTable.rowKeySet().iterator();
    while (wordItr.hasNext()) {
      String word = wordItr.next();
      Collection<Integer> wordGridCounts = wordGridCountTable.row(word).values();
      Long count = 0L;
      for (Integer wordGridCount : wordGridCounts) {
        count += wordGridCount;
      }
      if (count < 50) {
        wordItr.remove();
      } else {
        wordCountMap.put(word, count);
      }
    }
    System.out.println("wordGridCountTable.rowKeySet() size: "
        + wordGridCountTable.rowKeySet().size());
    System.out.println("wordCountMap size: " + wordCountMap.size());
  }

  // TODO: Speed up; this function takes a lot of time.
  private void buildWordAndGridProbabilityDistribution() {
    for (String word : wordGridCountTable.rowKeySet()) {
      Long wordCount = wordCountMap.get(word);
      for (Integer grid : wordGridCountTable.columnKeySet()) {
        double probability = 0.0;
        Integer wordCountInGrid = wordGridCountTable.get(word, grid);
        if (wordCountInGrid != null) {
          probability = ((double) wordCountInGrid) / wordCount;
        }
        wordGridProbDist.put(word, grid, probability);
      }
    }
  }
  
  private void buildWordAndNieghborhoodProbabilityDistribution() {
    SetMultimap<Integer, Integer> neighborhoodIdToGridIdMap = HashMultimap.create();
    for (Integer gridId : gridIdToNeighborhoodMap.keySet()) {
      neighborhoodIdToGridIdMap.put(gridIdToNeighborhoodMap.get(gridId), gridId);
    }
    for (String word : wordGridCountTable.rowKeySet()) {
      Long wordCount = wordCountMap.get(word);
      for (Integer neighborhoodId : neighborhoodIdToGridIdMap.keySet()) {
        Set<Integer> gridIds = neighborhoodIdToGridIdMap.get(neighborhoodId);
        double wordNeighborhoodCount = 0.0;
        for (Integer grid : gridIds) {
          Integer wordCountInGrid = wordGridCountTable.get(word, grid);
          if (wordCountInGrid != null) {
            wordNeighborhoodCount += wordCountInGrid;
          }
        }
        wordNeighborhoodProbDist.put(word, neighborhoodId, (wordNeighborhoodCount / wordCount));
      }
    }
  }
  
  private void writeWordGridProbilityDistribution(String outWordGridDistFilename)
      throws IOException {
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
        outWordGridDistFilename)))) {
      List<String> wordList = new ArrayList<String>(wordGridProbDist.rowKeySet());
      List<Integer> gridList = new ArrayList<Integer>(wordGridProbDist.columnKeySet());

      // Header line: grid IDs
      for (Integer gridId : gridList) {
        writer.append("," + gridId);
      }
      writer.append("\n");

      // Rest of the rows
      for (String word : wordList) {
        writer.append(word);
        for (Integer gridId : gridList) {
          writer.append("," + wordGridProbDist.get(word, gridId));
        }
        writer.append("\n");
      }
    }
  }
  
  private void writeNeighborhoodProbilityDistribution(String outNeighborhoodGridDistFilename)
      throws IOException {
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
        outNeighborhoodGridDistFilename)))) {
      List<String> wordList = new ArrayList<String>(wordNeighborhoodProbDist.rowKeySet());
      List<Integer> neighborhoodList = new ArrayList<Integer>(wordNeighborhoodProbDist.columnKeySet());

      // Header line: grid IDs
      for (Integer neighborhoodId : neighborhoodList) {
        writer.append("," + neighborhoodId);
      }
      writer.append("\n");

      // Rest of the rows
      for (String word : wordList) {
        writer.append(word);
        for (Integer neighborhoodId : neighborhoodList) {
          writer.append("," + wordNeighborhoodProbDist.get(word, neighborhoodId));
        }
        writer.append("\n");
      }
    }
  }
  
  public double findGridProbabilityGivenWords(Integer gridId, String tweet) {
    String[] words = tweet.split("\\s+");
    double logProb = Math.log(gridProbDist.get(gridId));
    for (String word : words) {
      Double wordGridProb = wordGridProbDist.get(word, gridId);
      if (wordGridProb != null && wordGridProb != 0) {
        logProb += Math.log(wordGridProb);
      }
    }
    return logProb;
  }
  
  public void readModelFromFiles(String gridProbDistFilename, String wordGridProbDistFilename,
      String wordNeighborhoodProbDistFilename) throws FileNotFoundException, IOException {
    readGridProbDist(gridProbDistFilename);
    readWordGridProbDist(wordGridProbDistFilename);
    readWordNeighborhoodProbDist(wordNeighborhoodProbDistFilename);
  }
  
  public void readGridProbDist(String gridProbDistFilename) throws IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(gridProbDistFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String[] lineParts = line.split(",");
        gridProbDist.put(Integer.parseInt(lineParts[0]), Double.parseDouble(lineParts[1]));
      }
    }
  }
  
  public void readWordGridProbDist(String wordGridProbDistFilename) throws IOException {
    List<Integer> gridIds = new ArrayList<Integer>();
    try (BufferedReader br = new BufferedReader(new FileReader(wordGridProbDistFilename))) {
      // Header line has grid IDs.
      String line = br.readLine();
      String[] lineParts = line.split(",");
      for (int i = 1; i < lineParts.length; i++) {
        gridIds.add(Integer.parseInt(lineParts[i]));
      }

      // Following lines are words followed by probabilities
      for (line = br.readLine(); line != null; line = br.readLine()) {
        lineParts = line.split(",");
        String word = lineParts[0];
        for (int i = 1; i < lineParts.length; i++) {
          wordGridProbDist.put(word, gridIds.get(i - 1), Double.parseDouble(lineParts[i]));
        }
      }
    }
  }
  
  public void readWordNeighborhoodProbDist(String wordNeighborhoodProbDistFilename) throws IOException {
    List<Integer> neighborhoodIds = new ArrayList<Integer>();
    try (BufferedReader br = new BufferedReader(new FileReader(wordNeighborhoodProbDistFilename))) {
      // Header line has grid IDs.
      String line = br.readLine();
      String[] lineParts = line.split(",");
      for (int i = 1; i < lineParts.length; i++) {
        neighborhoodIds.add(Integer.parseInt(lineParts[i]));
      }

      // Following lines are words followed by probabilities
      for (line = br.readLine(); line != null; line = br.readLine()) {
        lineParts = line.split(",");
        String word = lineParts[0];
        for (int i = 1; i < lineParts.length; i++) {
          wordGridProbDist.put(word, neighborhoodIds.get(i - 1), Double.parseDouble(lineParts[i]));
        }
      }
    }
  }
}
