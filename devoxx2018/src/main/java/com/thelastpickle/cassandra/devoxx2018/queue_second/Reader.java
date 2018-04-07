package com.thelastpickle.cassandra.devoxx2018.queue_second;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
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
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;



public final class Reader implements Runnable {
  private final int messagesToRead;
  private final int threadId;
  private final String uniqueId;

  public Reader(int messagesToRead, int threadId) {
    super();
    this.messagesToRead = messagesToRead;
    this.threadId = threadId;
    this.uniqueId = "alex-" + threadId;
  }

  @Override
  public void run() {
    Cluster cluster = Cluster.builder()
        .addContactPoint("54.200.68.60")
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
        .build();
    Session session = cluster.connect("devoxx");

    PreparedStatement readStatement = session
        .prepare(
            "SELECT id_queue, time_bucket, id_message, payload, published_by, processed_by FROM devoxx.messages_good WHERE id_queue = ? and time_bucket = ? limit 100");
    PreparedStatement ackStatement = session.prepare(
        "UPDATE devoxx.messages_ack SET payload = ?, published_by = ?, processed_by = processed_by + ? WHERE id_queue = ? AND id_message = ?");
    PreparedStatement deleteStatement = session.prepare("DELETE FROM devoxx.messages_good WHERE id_queue = ? and id_message = ? and time_bucket = ?");
    PreparedStatement lockStatement = session.prepare(
        "UPDATE devoxx.messages_good USING TTL 10 SET processed_by = ? WHERE id_queue = ? AND id_message = ? and time_bucket = ? IF processed_by = 'nobody'");

    List<ResultSetFuture> readFutures = Lists.newArrayList();
    int i = 0;
    while (i <= messagesToRead) {
      boolean processed = false;

      long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());

      for (int idQueue = 0; idQueue < 10; idQueue++) {
        readFutures.add(session.executeAsync(readStatement.bind(idQueue, minute - 1)));
        readFutures.add(session.executeAsync(readStatement.bind(idQueue, minute)));
      }

      ImmutableList<ListenableFuture<ResultSet>> results = Futures.inCompletionOrder(readFutures);

      for (ListenableFuture<ResultSet> result : results) {
        ResultSet resultSet;
        try {
          resultSet = result.get();
          toploop: for (Row row : resultSet) {
            if (row.getString("processed_by").equals("nobody")) {
              // on tente de "locker" l'enregistrement pour éviter que d'autres ne le traitent
              ResultSet lockResult = session
                  .execute(lockStatement.bind(uniqueId, row.getInt("id_queue"), row.getUUID("id_message"), row.getLong("time_bucket")));

              if (lockResult.wasApplied()) {
                // lock réussi
                session.execute(
                    ackStatement.bind(row.getString("payload"), row.getString("published_by"), Arrays.asList(uniqueId),
                        row.getInt("id_queue"),
                        row.getUUID("id_message")));
                session
                    .execute(deleteStatement.bind(row.getInt("id_queue"), row.getUUID("id_message"), row.getLong("time_bucket")));
                System.out.println("Traité : " + row.getUUID("id_message"));
                System.out.println("Messages lus et acquittés : " + (i + 1));
                ++i;
                processed = true;
                break toploop;
              }
            }
          }
        }
        catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
      if (!processed) {
        System.out.println("Pas de message (╯°□°)╯︵ ┻━┻");
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }


    cluster.close();

  }
}
