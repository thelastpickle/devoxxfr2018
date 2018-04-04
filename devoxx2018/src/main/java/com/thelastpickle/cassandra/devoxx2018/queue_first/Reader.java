package com.thelastpickle.cassandra.devoxx2018.queue_first;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;



public final class Reader {
  private static final String USER_ID = "alex";

  public static void read(int messagesToRead) throws InterruptedException {

    Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .build();
    Session session = cluster.connect("devoxx");

    PreparedStatement readStatement = session
        .prepare("SELECT id_queue, id_message, payload, published_by FROM devoxx.messages WHERE id_queue = ? limit 1");
    PreparedStatement ackStatement = session.prepare(
        "UPDATE devoxx.messages_ack SET payload = ?, published_by = ?, processed_by = processed_by + ? WHERE id_queue = ? AND id_message = ?");
    PreparedStatement deleteStatement = session.prepare("DELETE FROM devoxx.messages WHERE id_queue = ? and id_message = ?");

    for (int i = 0; i <= messagesToRead; i++) {
      int idQueue = i % 10;

      ResultSet resultSet = session.execute(readStatement.bind(idQueue));
      Row row = resultSet.one();
      if (row != null) {
        UUID idMessage = row.getUUID("id_message");

        // Delete async
        ResultSetFuture del = session.executeAsync(deleteStatement.bind(idQueue, idMessage));

        // ack sync
        session.execute(
            ackStatement.bind(row.getString("payload"), row.getString("published_by"), Arrays.asList(USER_ID), idQueue, idMessage));

        // on attend que le delete soit acquitté
        del.getUninterruptibly();

        System.out.println("Traité : " + idMessage);
        System.out.println("Messages lus et acquittés : " + (i + 1));
      }
      else {
        System.out.println("Pas de message :'(");
        Thread.sleep(1000);
      }
    }


    cluster.close();

  }
}
