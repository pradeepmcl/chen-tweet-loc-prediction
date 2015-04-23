package edu.ncsu.mas.platys.lbsn;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TweetLocationPredictionPipeline {

  private static final Set<Long> userIds = new HashSet<Long>();
  private static final Map<String, Integer> tweetIdToGridIdMap = new HashMap<String, Integer>();
  private static final Map<Integer, Integer> gridIdToNeighborhoodIdMap = new HashMap<Integer, Integer>();

  private static final DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  
  public static void main(String[] args) throws IOException, InstantiationException,
      IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException,
      ExecutionException {
    String inUserIdsFilename = args[0];
    
    String inGridFilename = args[1];
    String inNeighborhoodFilename = args[2];
    
    String splitDate = args[3]; // YYYY-MM-DD

    String outGridDistFilename = args[4];
    String outNeighborhoodDistFilename = args[5];
    
    String outWordGridDistFilename = args[6];
    String outWordNeighborhoodDistFilename = args[7];
    
    String outPredictionsFilename = args[8];

    String gridProbDistFilename = null;
    String neighborhoodProbDistFilename = null;
    String wordGridProbDistFilename = null;
    String wordNeighborhoodProbDistFilename = null;
    if (args.length > 9) {
      gridProbDistFilename = args[9];
      neighborhoodProbDistFilename = args[10];
      wordGridProbDistFilename = args[11];
      wordNeighborhoodProbDistFilename = args[12];
    }

    readUserIds(inUserIdsFilename);
    readTweetIdToGridId(inGridFilename);
    readGridIdToNeighborhoodId(inNeighborhoodFilename);

    TweetLocationLearner2 learner = new TweetLocationLearner2(Collections.unmodifiableSet(userIds),
        Collections.unmodifiableMap(tweetIdToGridIdMap), Collections.unmodifiableMap(gridIdToNeighborhoodIdMap));
    System.out.println("Strating training at " + df.format(Calendar.getInstance().getTime()));
    if (gridProbDistFilename != null && wordGridProbDistFilename != null) {
      learner.readModelFromFiles(gridProbDistFilename, neighborhoodProbDistFilename,
          wordGridProbDistFilename, wordNeighborhoodProbDistFilename);
    } else {
      learner.train(splitDate, outGridDistFilename, outNeighborhoodDistFilename,
          outWordGridDistFilename, outWordNeighborhoodDistFilename);
    }
    System.out.println("Ended training at " + df.format(Calendar.getInstance().getTime()));

    TweetLocationParallelTester2 tester = new TweetLocationParallelTester2(
        Collections.unmodifiableSet(userIds), Collections.unmodifiableMap(tweetIdToGridIdMap),
        Collections.unmodifiableMap(gridIdToNeighborhoodIdMap), learner);
    System.out.println("Strating testing at " + df.format(Calendar.getInstance().getTime()));
    tester.test(splitDate, outPredictionsFilename);
    System.out.println("Ended testing at " + df.format(Calendar.getInstance().getTime()));
  }

  private static void readTweetIdToGridId(String inGridFilename) throws FileNotFoundException,
      IOException {
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
  
  private static void readGridIdToNeighborhoodId(String inNeighborhoodFilename)
      throws FileNotFoundException, IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(inNeighborhoodFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: tweetID,lat1,lon1,lat2,lon2,gridId1,gridId2,...
        String[] lineParts = line.split(",");
        Integer gridId = Integer.parseInt(lineParts[0]);
        for (int i = 5; i < lineParts.length; i++) {
          gridIdToNeighborhoodIdMap.put(gridId, Integer.parseInt(lineParts[i]));
        }
      }
    }
    System.out.println("GridIdToNeighborhoodIdMap size: " + gridIdToNeighborhoodIdMap.size());
  }

  private static void readUserIds(String inUserIdsFilename) throws FileNotFoundException,
      IOException {
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
}
