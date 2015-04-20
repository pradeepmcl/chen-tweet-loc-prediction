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

public class TweetLocationPredictionPipeline {

  private static final Set<Long> userIds = new HashSet<Long>();
  private static final Map<String, Integer> tweetIdToGridIdMap = new HashMap<String, Integer>();

  private static final DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  
  public static void main(String[] args) throws IOException, InstantiationException,
      IllegalAccessException, ClassNotFoundException, SQLException {
    String inUserIdsFilename = args[0];
    String inGridFilename = args[1];
    String splitDate = args[2]; // YYYY-MM-DD

    String outGridDistFilename = args[3];
    String ouWordCountsFilename = args[4];
    String outPredictionsFilename = args[5];
    
    String gridProbDistFilename = null;
    String wordGridProbDistFilename = null;
    if (args.length > 6) {
      gridProbDistFilename = args[6];
      wordGridProbDistFilename = args[7];
    }

    readUserIds(inUserIdsFilename);
    readTweetIdToGridId(inGridFilename);

    TweetLocationLearner learner = new TweetLocationLearner(Collections.unmodifiableSet(userIds),
        Collections.unmodifiableMap(tweetIdToGridIdMap));
    System.out.println("Strating training at " + df.format(Calendar.getInstance().getTime()));
    if (gridProbDistFilename != null && wordGridProbDistFilename != null) {
      learner.readModelFromFiles(gridProbDistFilename, wordGridProbDistFilename);
    } else {
      learner.train(splitDate, outGridDistFilename, ouWordCountsFilename);
    }
    System.out.println("Ended training at " + df.format(Calendar.getInstance().getTime()));

    TweetLocationTester tester = new TweetLocationTester(Collections.unmodifiableSet(userIds),
        Collections.unmodifiableMap(tweetIdToGridIdMap));
    System.out.println("Strating testing at " + df.format(Calendar.getInstance().getTime()));
    tester.test(learner, splitDate, outPredictionsFilename);
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
