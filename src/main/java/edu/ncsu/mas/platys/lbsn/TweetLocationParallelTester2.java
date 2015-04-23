package edu.ncsu.mas.platys.lbsn;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.ncsu.mas.platys.lbsn.db.TweetDbHandler;

public class TweetLocationParallelTester2 {
  private final Map<String, Integer> tweetIdToGridIdMap;
  private final Set<Long> userIds;
  
  private final TweetLocationLearner2 learner;
  private final Map<Integer, Double> gridProbabDist;
  
  public TweetLocationParallelTester2(Set<Long> userIds, Map<String, Integer> tweetIdToGridIdMap,
      TweetLocationLearner2 learner) {
    this.tweetIdToGridIdMap = tweetIdToGridIdMap;
    this.userIds = userIds;
    this.learner = learner;
    this.gridProbabDist = learner.getGridProbabDist();
  }
  
  public void test(String splitDate, String outPredictionsFilename)
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException,
      IOException, InterruptedException, ExecutionException {
    ExecutorService gridFinderService = Executors.newFixedThreadPool(Runtime.getRuntime()
        .availableProcessors());
    CompletionService<Grid> gridFinderCompletionSerive = new ExecutorCompletionService<Grid>(
        gridFinderService);
    
    try (TweetDbHandler dbHandler = new TweetDbHandler();
        Statement st = dbHandler.getConnection().createStatement()) {
      int pageSize = 10000;
      st.setFetchSize(pageSize);
      st.setMaxRows(pageSize);
      for (long pageNum = 0; true; pageNum++) {
        try (ResultSet rs = st.executeQuery("select t2.user_id, t1.tweet_id, t1.content "
            + "from tweet_topic t1, tweet_venue_new t2 " 
            + "where t2.creation_time >= '" + splitDate + "' and t1.tweet_id = t2.tweet_id " 
            + "limit " + (pageNum * pageSize) + ", " + pageSize)) {
          if (!rs.next()) {
            break;
          }
          int tasksSubmitted = 0;
          do {
            long userId = rs.getLong(1);
            if (userIds.contains(userId)) {
              String tweetId = rs.getString(2);
              String content = rs.getString(3);
              gridFinderCompletionSerive.submit(new GridFinder(userId, tweetId, content));
              tasksSubmitted++;
            }
          } while (rs.next());

          try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
              outPredictionsFilename, true)))) {
            for (int i = 0; i < tasksSubmitted; i++) {
              Grid predictedGrid = gridFinderCompletionSerive.take().get();
              int actualGridId = tweetIdToGridIdMap.get(predictedGrid.tweetId);
              writer.println(predictedGrid.userId + "," + predictedGrid.tweetId + ","
                  + actualGridId + "," + predictedGrid.gridId);
            }
          }
        }
      }
    }
    // There should be no tasks left at this stage.
    gridFinderService.shutdown();
  }
  
  public class GridFinder implements Callable<Grid> {
    private Long userId;
    private String tweetId;
    private String content;
    
    public GridFinder(Long userId, String tweetId, String content) {
      this.userId = userId;
      this.tweetId = tweetId;
      this.content = content;
    }
    
    @Override
    public Grid call() throws Exception {
      Grid predictedGrid = new Grid(userId, tweetId);
      for (Integer gridId : gridProbabDist.keySet()) {
        double tempPredictedProb = learner.findGridProbabilityGivenWords(gridId, content);
        if (tempPredictedProb > predictedGrid.gridProbability) {
          predictedGrid.setPredictedGrid(gridId, tempPredictedProb);
        }
      }
      return predictedGrid;
    }
  }
  
  public static class Grid {
    Long userId;
    String tweetId;
    Integer gridId = -1;
    Double gridProbability = Double.NEGATIVE_INFINITY;

    public Grid(Long userId, String tweetId) {
      this.userId = userId;
      this.tweetId = tweetId;
    }
    
    public void setPredictedGrid(Integer gridId, Double gridProbability) {
      this.gridId = gridId;
      this.gridProbability = gridProbability;
    }
  }
}
