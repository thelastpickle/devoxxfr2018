package com.thelastpickle.cassandra.devoxx2018.queue_first;

import java.util.Arrays;
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



public final class Reader implements Runnable {
  private final int messagesToRead;

  public Reader(int messagesToRead) {
    super();
    this.messagesToRead = messagesToRead;
  }

  @Override
  public void run() {
    Cluster cluster = Cluster.builder()
        .addContactPoint("52.43.224.73")
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
        .build();
    Session session = cluster.connect("devoxx");

    PreparedStatement readStatement = session
        .prepare("SELECT id_queue, id_message, payload, published_by FROM devoxx.messages WHERE id_queue = ? limit 1");
    PreparedStatement ackStatement = session.prepare(
        "UPDATE devoxx.messages_ack SET payload = ?, published_by = ?, processed_by = processed_by + ? WHERE id_queue = ? AND id_message = ?");
    PreparedStatement deleteStatement = session.prepare("DELETE FROM devoxx.messages WHERE id_queue = ? and id_message = ?");

    for (int i = 0; i <= messagesToRead; i++) {
      int idQueue = 1;
      try {
        ResultSet resultSet = session.execute(readStatement.bind(idQueue));
        Row row = resultSet.one();
        if (row != null) {
          UUID idMessage = row.getUUID("id_message");

          // Delete async
          ResultSetFuture del = session.executeAsync(deleteStatement.bind(idQueue, idMessage));

          // ack sync
          session.execute(
              ackStatement.bind(row.getString("payload"), row.getString("published_by"), Arrays.asList(Writer.USER_ID), idQueue, idMessage));

          // on attend que le delete soit acquitté
          del.getUninterruptibly();

          System.out.println("Traité : " + idMessage);
          System.out.println("Messages lus et acquittés : " + (i + 1));
        }
        else {
          System.out.println("Pas de message :'(");
          try {
            Thread.sleep(1000);
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      catch (Exception e) {
        System.out.println("Erreur pendant le traitement des messages");
        e.printStackTrace();
      }
    }

    cluster.close();

  }
}
