package com.thelastpickle.cassandra.devoxx2018.movielens;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

public class Loader {
  public static void main(String[] args) throws InterruptedException {

    Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
        .build();
    Session session = cluster.connect("movielens");

    PreparedStatement insertStatement = session
        .prepare("INSERT INTO movies_by_first_letter (first_letter, first_word, id, avg_rating, genres, name, release_date, url, video_release_date) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

    ResultSet movies = session.execute("SELECT * FROM movies");

    List<ResultSetFuture> futures = Lists.newArrayList();
    int i = 0;

    for (Row movie:movies) {

      futures.add(
          session.executeAsync(
              insertStatement.bind(
                  movie.getString("name").substring(0, 1),
                  movie.getString("name").split(" ")[0],
                  movie.getUUID("id"),
                  movie.getFloat("avg_rating"),
                  movie.getSet("genres", String.class),
                  movie.getString("name"),
                  movie.getDate("release_date"),
                  movie.getString("url"),
                  movie.getDate("video_release_date"))));
      ++i;

      if (futures.size() % 100 == 0) {
        Futures.successfulAsList(futures);
        System.out.println(i + " processed movies...");
        futures = Lists.newArrayList();
      }
    }

    Futures.successfulAsList(futures);
    System.out.println(i + " processed movies...");
    cluster.close();
  }

}
