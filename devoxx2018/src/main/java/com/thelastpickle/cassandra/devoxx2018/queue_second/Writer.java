package com.thelastpickle.cassandra.devoxx2018.queue_second;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;



public final class Writer {
  private static final String USER_ID = "alex";

  public static void write(int messagesToInsert, int rateLimit) {
    RateLimiter rateLimiter = RateLimiter.create(rateLimit);

    Cluster cluster = Cluster.builder()
        .addContactPoint("54.200.68.60")
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .build();
    Session session = cluster.connect("devoxx");

    PreparedStatement writeStatement = session
        .prepare("INSERT INTO devoxx.messages_good(id_queue, id_message, payload, published_by, time_bucket, processed_by) values(?,?,?,?,?,?)");

    List<ResultSetFuture> futures = Lists.newArrayList();
    for (int i = 0; i <= messagesToInsert; i++) {
      UUID idMessage = UUIDs.timeBased();
      int idQueue = i % 10;
      String payload = RandomStringUtils.random(50, true, true);

      rateLimiter.acquire();
      futures.add(session
          .executeAsync(
              writeStatement.bind(idQueue, idMessage, payload, USER_ID, TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()), "nobody")));

      if (i % 100 == 0) {
        // no DDoS policy
        Futures.successfulAsList(futures);
        futures = Lists.newArrayList();
        System.out.println("Inserted " + i + " rows...");
      }
    }

    Futures.successfulAsList(futures);
    System.out.println("Inserted " + messagesToInsert + " rows...");

    cluster.close();

  }
}
