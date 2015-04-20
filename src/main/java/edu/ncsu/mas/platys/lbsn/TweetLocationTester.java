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

import edu.ncsu.mas.platys.lbsn.db.TweetDbHandler;

public class TweetLocationTester {
  private final Map<String, Integer> tweetIdToGridIdMap;
  private final Set<Long> userIds;
  
  public TweetLocationTester(Set<Long> userIds, Map<String, Integer> tweetIdToGridIdMap) {
    this.tweetIdToGridIdMap = tweetIdToGridIdMap;
    this.userIds = userIds;
  }
  
  public void test(TweetLocationLearner learner, String splitDate, String outPredictionsFilename)
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException,
      IOException {
    Map<Integer, Double> gridProbabDist = learner.getGridProbabDist();

    try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
          outPredictionsFilename)));
        TweetDbHandler dbHandler = new TweetDbHandler();
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
          do {
            long userId = rs.getLong(1);
            if (userIds.contains(userId)) {
              String tweetId = rs.getString(2);
              int actualGridId = tweetIdToGridIdMap.get(tweetId);
              int predictedGridId = -1;
              double predictedGridProb = 0.0;
              for (Integer gridId : gridProbabDist.keySet()) {
                double tempPredictedProb = learner.findGridProbabilityGivenWords(gridId,
                    rs.getString(3));
                if (tempPredictedProb > predictedGridProb) {
                  predictedGridProb = tempPredictedProb;
                  predictedGridId = gridId;
                }
              }
              writer.println(userId + "," + tweetId + "," + actualGridId + "," + predictedGridId);
            }
          } while (rs.next());
        }
      }
    }
  }
}
