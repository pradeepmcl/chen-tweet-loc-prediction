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
  private static final Set<String> localWords = new HashSet<String>();

  private static final DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

  public static void main(String[] args) throws IOException, InstantiationException,
      IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException,
      ExecutionException {
    String inUserIdsFilename = args[0];
    
    String inGridFilename = args[1];
    String inNeighborhoodFilename = args[2];
    String inStateFilterFilename = args[3];
    String inLocalWordsParamsFilename = args[4];
    String splitDate = args[5]; // YYYY-MM-DD

    String outGridDistFilename = args[6];
    String outNeighborhoodDistFilename = args[7];
    String outWordGridDistFilename = args[8];
    String outWordNeighborhoodDistFilename = args[9];
    String outPredictionsFilename = args[10];

    String gridProbDistFilename = null;
    String neighborhoodProbDistFilename = null;
    String wordGridProbDistFilename = null;
    String wordNeighborhoodProbDistFilename = null;

    if (args.length > 11) {
      gridProbDistFilename = args[11];
      neighborhoodProbDistFilename = args[12];
      wordGridProbDistFilename = args[13];
      wordNeighborhoodProbDistFilename = args[14];
    }

    readUserIds(inUserIdsFilename, inStateFilterFilename);
    readTweetIdToGridId(inGridFilename, inStateFilterFilename);
    readGridIdToNeighborhoodId(inNeighborhoodFilename);
    readLocalWords(inLocalWordsParamsFilename);

    TweetLocationLearner2 learner = new TweetLocationLearner2(Collections.unmodifiableSet(userIds),
        Collections.unmodifiableMap(tweetIdToGridIdMap),
        Collections.unmodifiableMap(gridIdToNeighborhoodIdMap),
        Collections.unmodifiableSet(localWords));
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

  private static void readTweetIdToGridId(String inGridFilename, String stateFilterFilename)
      throws FileNotFoundException, IOException {
    Set<String> tweetsToRetain = new HashSet<String>();
    try (BufferedReader br = new BufferedReader(new FileReader(stateFilterFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: tweetID,userId
        String[] lineParts = line.split(",");
        tweetsToRetain.add(lineParts[0]);
      }
    }
    System.out.println("tweetsToRetain size: " + tweetsToRetain.size()); // TODO: Remove
    
    try (BufferedReader br = new BufferedReader(new FileReader(inGridFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: tweetID,latitude,longitude,userID,gridID
        String[] lineParts = line.split(",");
        if (tweetsToRetain.contains(lineParts[0])) {
          tweetIdToGridIdMap.put(lineParts[0], Integer.parseInt(lineParts[4]));
        }
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
        // Line format: neighborhoodId,lat1,lon1,lat2,lon2,gridId1,gridId2,...
        String[] lineParts = line.split(",");
        Integer neighborhoodId = Integer.parseInt(lineParts[0]);
        for (int i = 5; i < lineParts.length; i++) {
          gridIdToNeighborhoodIdMap.put(Integer.parseInt(lineParts[i]), neighborhoodId);
        }
      }
    }
    System.out.println("GridIdToNeighborhoodIdMap size: " + gridIdToNeighborhoodIdMap.size());
  }

  private static void readUserIds(String inUserIdsFilename, String stateFilterFilename)
      throws FileNotFoundException, IOException {
    Set<String> usersToRetain = new HashSet<String>();
    try (BufferedReader br = new BufferedReader(new FileReader(stateFilterFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: tweetID,userId
        String[] lineParts = line.split(",");
        usersToRetain.add(lineParts[1]);
      }
    }
    
    try (BufferedReader br = new BufferedReader(new FileReader(inUserIdsFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: userId
        String userId = line.trim();
        if (usersToRetain.contains(userId)) {
          userIds.add(Long.parseLong(userId));
        }
      }
    }
    System.out.println("userIds size: " + userIds.size());
  }

  private static void readLocalWords(String inLocalWordsParamsFilename) throws FileNotFoundException,
      IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(inLocalWordsParamsFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: word,c,alpha
        String[] lineParts = line.split(",");
        String word = lineParts[0].replace("word_", "");
        double c = Double.parseDouble(lineParts[1]);
        double alpha = Double.parseDouble(lineParts[2]);
        if ( c > 0 && alpha > 1) { // TODO Send c and alpha cutoffs as parameters
          localWords.add(word);
        }
      }
    }
    System.out.println("userIds size: " + userIds.size());
  }

}
