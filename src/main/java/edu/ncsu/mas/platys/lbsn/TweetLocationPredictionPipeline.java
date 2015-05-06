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
    String inLocalWordsParamsFilename = args[3];
    String splitDate = args[4]; // YYYY-MM-DD

    String outGridDistFilename = args[5];
    String outNeighborhoodDistFilename = args[6];
    String outWordGridDistFilename = args[7];
    String outWordNeighborhoodDistFilename = args[8];
    String outPredictionsFilename = args[9];

    String gridProbDistFilename = null;
    String wordGridProbDistFilename = null;
    String wordNeighborhoodProbDistFilename = null;
    if (args.length > 10) {
      gridProbDistFilename = args[10];
      wordGridProbDistFilename = args[11];
      wordNeighborhoodProbDistFilename = args[12];
    }

    readUserIds(inUserIdsFilename);
    readTweetIdToGridId(inGridFilename);
    readGridIdToNeighborhoodId(inNeighborhoodFilename);
    readLocalWords(inLocalWordsParamsFilename);

    TweetLocationLearner2 learner = new TweetLocationLearner2(Collections.unmodifiableSet(userIds),
        Collections.unmodifiableMap(tweetIdToGridIdMap),
        Collections.unmodifiableMap(gridIdToNeighborhoodIdMap),
        Collections.unmodifiableSet(localWords));
    System.out.println("Strating training at " + df.format(Calendar.getInstance().getTime()));
    if (gridProbDistFilename != null && wordGridProbDistFilename != null) {
      learner.readModelFromFiles(gridProbDistFilename, wordGridProbDistFilename,
          wordNeighborhoodProbDistFilename);
    } else {
      learner.train(splitDate, outGridDistFilename, outNeighborhoodDistFilename,
          outWordGridDistFilename, outWordNeighborhoodDistFilename);
    }
    System.out.println("Ended training at " + df.format(Calendar.getInstance().getTime()));

    TweetLocationParallelTester2 tester = new TweetLocationParallelTester2(
        Collections.unmodifiableSet(userIds), Collections.unmodifiableMap(tweetIdToGridIdMap),
        learner);
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
