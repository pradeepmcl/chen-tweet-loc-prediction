package edu.ncsu.mas.platys.lbsn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class LocalWordLearner {
  private static final Map<Integer, Integer> gridIdToNeighborhoodIdMap = new HashMap<Integer, Integer>();
  private static final Table<String, Integer, Double> wordGridProbDist = HashBasedTable.create();
  private static final Table<String, Integer, Double> wordNeighborhoodProbDist = HashBasedTable
      .create();

  private static final Map<Integer, Double[]> gridIdToCorner = new HashMap<Integer, Double[]>();
  private static final Map<Integer, Double[]> neighborhoodIdToCorner = new HashMap<Integer, Double[]>();

  public static void main(String[] args) throws FileNotFoundException, IOException,
      InterruptedException, ExecutionException {
    String inNeighborhoodFilename = args[0];
    String wordGridProbDistFilename = args[1];
    String wordNeighborhoodProbDistFilename = args[2];
    String gridIdToCornersFile = args[3];

    File outDir = new File(args[4]);

    readGridIdToNeighborhoodId(inNeighborhoodFilename);
    System.out.println("GridIdToNeighborhoodIdMap size: " + gridIdToNeighborhoodIdMap.size());
    
    readWordGridProbDist(wordGridProbDistFilename);
    System.out.println("wordGridProbDist size: " + wordGridProbDist.size());
    
    readWordNeighborhoodProbDist(wordNeighborhoodProbDistFilename);
    System.out.println("wordNeighborhoodProbDist size: " + wordNeighborhoodProbDist.size());
    System.out.println("neighborhoodIdToCorner size: " + neighborhoodIdToCorner.size());
    
    readGridIdToCorner(gridIdToCornersFile);
    System.out.println("gridIdToCorner size: " + gridIdToCorner.size());

    ExecutorService locaFunComputerService = Executors.newFixedThreadPool(Runtime.getRuntime()
        .availableProcessors());
    CompletionService<LocationFunctionParameters> locFunCompletionSerive = new ExecutorCompletionService<LocationFunctionParameters>(
        locaFunComputerService);

    for (String word : wordNeighborhoodProbDist.rowKeySet()) {
      locFunCompletionSerive.submit(new LocationFunctionComputer(word));
    }
    
    locaFunComputerService.shutdown();

    for (int i = 0; i < wordNeighborhoodProbDist.rowKeySet().size(); i++) {
      LocationFunctionParameters locFun = locFunCompletionSerive.take().get();
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outDir, "word_"
          + locFun.word + ".m")))) {
        writer.write("function f = word_" + locFun.word + "(x)");
        writer.newLine();
        writer.write("f =");
        for (Double distance : locFun.insideDistances) {
          writer.write(" - log(x(1) * (" + distance + "^-x(2)))");
        }        
        for (Double distance : locFun.outsideDistances) {
          writer.write(" - log(1 - x(1) * (" + distance + "^-x(2)))");
        }
        writer.newLine();
      }
    }
  }

  private static class LocationFunctionComputer implements Callable<LocationFunctionParameters> {
    private final LocationFunctionParameters locFunParams;

    public LocationFunctionComputer(String word) {
      locFunParams = new LocationFunctionParameters(word);
    }

    @Override
    public LocationFunctionParameters call() {
      // Find representative neighborhood ID
      Integer repId = findRepresentativeNieghborhoodId(locFunParams.word);
      if (repId == -1) {
        throw new IllegalArgumentException("Word: " + locFunParams.word
            + " has no representative grid");
      }
      locFunParams.representativeNeighborhoodId = repId;

      // Find grid Ids containing the word
      findGridsContainingWord(locFunParams);

      return locFunParams;
    }
  }

  private static Integer findRepresentativeNieghborhoodId(String word) {
    Integer repId = -1;
    Double repProb = 0.0;
    Map<Integer, Double> wordProbs = wordNeighborhoodProbDist.row(word);
    for (Entry<Integer, Double> wordProbEntry : wordProbs.entrySet()) {
      if (wordProbEntry.getValue() > repProb) {
        repProb = wordProbEntry.getValue();
        repId = wordProbEntry.getKey();
      }
    }
    return repId;
  }

  private static void findGridsContainingWord(LocationFunctionParameters locFunParams) {
    for (Entry<Integer, Double> gridEntry : wordGridProbDist.row(locFunParams.word).entrySet()) {
      if (gridEntry.getValue() > 0.0) {
        double distance = findDistance(
            neighborhoodIdToCorner.get(locFunParams.representativeNeighborhoodId),
            gridIdToCorner.get(gridIdToNeighborhoodIdMap.get(gridEntry.getKey())));
        if (gridIdToNeighborhoodIdMap.get(gridEntry.getKey()).equals(
            locFunParams.representativeNeighborhoodId)) {
          locFunParams.insideDistances.add(distance);
          // locFunParams.gridIdsInsideNeighborhood.add(gridEntry.getKey());
        } else {
          locFunParams.outsideDistances.add(distance);
          // locFunParams.gridIdsOutsideNeighborhood.add(gridEntry.getKey());
        }
      }
    }
  }

  private static double findDistance(Double[] latlng1, Double[] latlng2) {
    double earthRadius = 6371; // kilo meters
    double dLat = Math.toRadians(latlng2[0] - latlng1[0]);
    double dLng = Math.toRadians(latlng2[1] - latlng1[1]);
    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(Math.toRadians(latlng1[0]))
        * Math.cos(Math.toRadians(latlng2[0])) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    BigDecimal dist = new BigDecimal(earthRadius * c);
    dist = dist.setScale(1, RoundingMode.CEILING);

    return dist.doubleValue();
  }

  private static class LocationFunctionParameters {
    String word;
    Integer representativeNeighborhoodId;
    final Set<Double> insideDistances = new HashSet<Double>();
    final Set<Double> outsideDistances = new HashSet<Double>();
    // final Set<Integer> gridIdsInsideNeighborhood = new HashSet<Integer>();
    // final Set<Integer> gridIdsOutsideNeighborhood = new HashSet<Integer>();

    public LocationFunctionParameters(String word) {
      this.word = word;
    }

  }

  private static void readGridIdToCorner(String gridIdToCornersFile) throws IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(gridIdToCornersFile))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        // Line format: gridId,lat1,lon1,lat2,lon2
        String[] lineParts = line.split(",");
        gridIdToCorner.put(Integer.parseInt(lineParts[0]),
            new Double[] { Double.parseDouble(lineParts[1]), Double.parseDouble(lineParts[2]) });
      }
    }

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

        neighborhoodIdToCorner.put(neighborhoodId, new Double[] {
            Double.parseDouble(lineParts[1]), Double.parseDouble(lineParts[2]) });
        // Double.parseDouble(lineParts[3]), Double.parseDouble(lineParts[4]) });

        for (int i = 5; i < lineParts.length; i++) {
          gridIdToNeighborhoodIdMap.put(Integer.parseInt(lineParts[i]), neighborhoodId);
        }
      }
    }
  }

  public static void readWordGridProbDist(String wordGridProbDistFilename) throws IOException {
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
          Double prob = Double.parseDouble(lineParts[i]);
          if (prob != 0.0) {
            wordGridProbDist.put(word, gridIds.get(i - 1), prob);
          }
        }
      }
    }
  }

  public static void readWordNeighborhoodProbDist(String wordNeighborhoodProbDistFilename)
      throws IOException {
    List<Integer> neighborhoodIds = new ArrayList<Integer>();
    try (BufferedReader br = new BufferedReader(new FileReader(wordNeighborhoodProbDistFilename))) {
      // Header line has neighborhood IDs.
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
          Double prob = Double.parseDouble(lineParts[i]);
          if (prob != 0.0) {
            wordNeighborhoodProbDist.put(word, neighborhoodIds.get(i - 1), prob);
          }
        }
      }
    }
  }
}
