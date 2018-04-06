package com.thelastpickle.cassandra.devoxx2018.queue_first;

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



public final class Reader {
  private static final String USER_ID = "alex";

  public static void read(int messagesToRead) throws InterruptedException, ExecutionException {

    Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
        .build();
    Session session = cluster.connect("devoxx");

    PreparedStatement readStatement = session
        .prepare(
            "SELECT id_queue, time_bucket, id_message, payload, published_by FROM devoxx.messages WHERE id_queue = ? and time_bucket = ? limit 1");
    PreparedStatement ackStatement = session.prepare(
        "UPDATE devoxx.messages_ack SET payload = ?, published_by = ?, processed_by = processed_by + ? WHERE id_queue = ? AND id_message = ?");
    PreparedStatement deleteStatement = session.prepare("DELETE FROM devoxx.messages WHERE id_queue = ? and id_message = ? and time_bucket = ?");
    PreparedStatement lockStatement = session.prepare(
        "UPDATE devoxx.messages SET processed_by = ? WHERE id_queue = ? AND id_message = ? and time_bucket = ? IF processed_by = null");

    List<ResultSetFuture> readFutures = Lists.newArrayList();
    int i = 0;
    while (i <= messagesToRead) {

      long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());

      for (int idQueue = 0; idQueue < 10; idQueue++) {
        readFutures.add(session.executeAsync(readStatement.bind(idQueue, minute - 1)));
        readFutures.add(session.executeAsync(readStatement.bind(idQueue, minute)));
      }

      ImmutableList<ListenableFuture<ResultSet>> results = Futures.inCompletionOrder(readFutures);

      for (ListenableFuture<ResultSet> result : results) {
        for (Row row : result.get()) {
          ResultSet lockResult = session
              .execute(lockStatement.bind(USER_ID, row.getInt("id_queue"), row.getUUID("message_id"), row.getLong("time_bucket")));
          if (lockResult.wasApplied()) {
            ResultSetFuture del = session
                .executeAsync(deleteStatement.bind(row.getInt("id_queue"), row.getUUID("message_id"), row.getLong("time_bucket")));
            session.execute(
                ackStatement.bind(row.getString("payload"), row.getString("published_by"), Arrays.asList(USER_ID), row.getInt("id_queue"),
                    row.getUUID("message_id")));
            del.getUninterruptibly();
            System.out.println("Traité : " + row.getUUID("message_id"));
            System.out.println("Messages lus et acquittés : " + (i + 1));
            ++i;
            break;
          }
        }
      }
      System.out.println("Pas de message :'(");
      Thread.sleep(1000);
    }


    cluster.close();

  }
}
