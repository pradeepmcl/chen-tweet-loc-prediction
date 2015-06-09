package edu.ncsu.mas.platys.lbsn;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GridPredictionAnalyzer {

  private static final Set<Long> usersWithHistory = new HashSet<Long>();
  private static final Set<Long> usersWithoutHistory = new HashSet<Long>();
  private static final Map<Integer, Integer> gridIdToNeighborhoodIdMap = new HashMap<Integer, Integer>();

  private static final Set<Double> historyDistances = new HashSet<Double>();
  private static final Set<Double> noHistoryDistances = new HashSet<Double>();

  private static int historyTotal = 0;
  private static int noHistoryTotal = 0;
  private static int historyTop1 = 0;
  private static int noHistoryTop1 = 0;
  private static int historyNbrhoodTop1 = 0;
  private static int noHistoryNbrhoodTop1 = 0;

  public static void main(String[] args) throws FileNotFoundException, IOException {
    String usersWithHistoryFilename = args[0];
    String usersWithoutHistoryFilename = args[1];
    String gridPredictionsInFilename = args[2];
    String distancePredictionsInFilename = args[3];
    String gridIdToneighborhoodIdInFilename = args[4];

    readUsers(usersWithHistoryFilename, usersWithHistory);
    System.out.println("usersWithHistory size: " + usersWithHistory.size());

    readUsers(usersWithoutHistoryFilename, usersWithoutHistory);
    System.out.println("usersWithoutHistory size: " + usersWithoutHistory.size());

    readDistancePredictions(distancePredictionsInFilename);
    System.out.println("History Avg Distance: " + findAverage(historyDistances));
    System.out.println("No History Avg Distance: " + findAverage(noHistoryDistances));

    readGridIdToNeighborhoodId(gridIdToneighborhoodIdInFilename);
    System.out.println("GridIdToNeighborhoodIdMap size: " + gridIdToNeighborhoodIdMap.size());

    readGridPredictions(gridPredictionsInFilename);
    System.out.println("History Top 1: " + (historyTop1 / historyTotal));
    System.out.println("No History Top 1: " + (noHistoryTop1 / noHistoryTotal));

    System.out.println("History Neighborhood Top 1: " + (historyNbrhoodTop1 / historyTotal));
    System.out.println("No History Neighborhood Top 1: " + (noHistoryNbrhoodTop1 / noHistoryTotal));

  }

  private static void readGridPredictions(String gridPredictionsInFilename)
      throws FileNotFoundException, IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(gridPredictionsInFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: userId,tweetId,predictedGridId,actualGridId
        String[] lineParts = line.split(",");
        Long userId = Long.parseLong(lineParts[0]);
        Integer predictedGridId = Integer.parseInt(lineParts[2]);
        Integer actualGridId = Integer.parseInt(lineParts[3]);
        Integer predictedNbrhoodId = gridIdToNeighborhoodIdMap.get(predictedGridId);
        Integer actualNbrhoodId = gridIdToNeighborhoodIdMap.get(actualGridId);
        if (usersWithHistory.contains(userId)) {
          if (predictedGridId.equals(actualGridId)) {
            historyTop1++;
          }
          if (predictedNbrhoodId.equals(actualNbrhoodId)) {
            historyNbrhoodTop1++;
          }
          historyTotal++;
        } else if (usersWithoutHistory.contains(userId)) {
          if (predictedGridId.equals(actualGridId)) {
            noHistoryTop1++;
          }
          if (predictedNbrhoodId.equals(actualNbrhoodId)) {
            noHistoryNbrhoodTop1++;
          }
          noHistoryTotal++;
        }
      }
    }
  }

  private static void readDistancePredictions(String distancePredictionsInFilename)
      throws FileNotFoundException, IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(distancePredictionsInFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: userId,gridId,distance
        String[] lineParts = line.split(",");
        Long userId = Long.parseLong(lineParts[0]);
        if (usersWithHistory.contains(userId)) {
          historyDistances.add(Double.parseDouble(lineParts[2]));
        } else if (usersWithoutHistory.contains(userId)) {
          noHistoryDistances.add(Double.parseDouble(lineParts[2]));
        }
      }
    }
  }

  private static void readUsers(String usersFilename, Set<Long> userIds)
      throws FileNotFoundException, IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(usersFilename))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: userId
        userIds.add(Long.parseLong(line.trim()));
      }
    }
  }

  // TODO: Overflow possible?
  private static Double findAverage(Set<Double> dists) {
    double sum = 0.0;
    for (Double dist : dists) {
      sum += dist;
    }
    return sum / dists.size();
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
  }
}
