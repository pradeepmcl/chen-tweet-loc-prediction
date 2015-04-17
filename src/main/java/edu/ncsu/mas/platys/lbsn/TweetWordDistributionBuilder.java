package edu.ncsu.mas.platys.lbsn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

public class TweetWordDistributionBuilder {

  // The following two maps are entangled :P
  private static final Map<Long, Integer> tweetIdToGridIdMap = new HashMap<Long, Integer>();
  private static final SetMultimap<Integer, Long> gridIdToTweetIdMap = HashMultimap.create();

  private static final Map<Integer, Double> gridProbabDist = new HashMap<Integer, Double>();

  public static void main(String[] args) throws FileNotFoundException, IOException {
    String inGridFilename = args[0];
    String outGridDistFilename = args[1];
    
    TweetWordDistributionBuilder distBldr = new TweetWordDistributionBuilder();

    distBldr.readTweetIdToGridId(inGridFilename);
    distBldr.findGridProbabilityDistribution();
    distBldr.writeGridProbabilityDistribution(outGridDistFilename);
  }

  private void findGridProbabilityDistribution() {
    for (Integer gridId : gridIdToTweetIdMap.keys()) {
      Set<Long> tweetIds = gridIdToTweetIdMap.get(gridId);
      gridProbabDist.put(gridId, ((double) tweetIds.size()) / tweetIdToGridIdMap.size());
    }
  }

  /*
  private void findWordsGivenGridProbabilityDistribution() {
    // TODO
  }

  private void findGridGivenWordsProbablity() {
    // TODO
  }
  */
  
  private void readTweetIdToGridId(String inGridFilename) throws FileNotFoundException, IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(inGridFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: tweetID,latitude,longitude,userID,gridID
        String[] lineParts = line.split(",");
        tweetIdToGridIdMap.put(Long.parseLong(lineParts[0]), Integer.parseInt(lineParts[4]));
        gridIdToTweetIdMap.put(Integer.parseInt(lineParts[4]), Long.parseLong(lineParts[0]));
      }
    }
  }
  
  private void writeGridProbabilityDistribution(String outGridDistFilename) throws IOException {
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outGridDistFilename)))) {
      for (Integer gridId : gridProbabDist.keySet()) {
        writer.println(gridId + "," + gridProbabDist.get(gridId));
      }
    }
  }
}
