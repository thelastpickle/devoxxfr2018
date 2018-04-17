package com.thelastpickle.cassandra.devoxx2018.queue_second;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;



public final class Reader implements Runnable {
  private final int messagesToRead;
  private final String uniqueId;

  public Reader(int messagesToRead, int threadId) {
    super();
    this.messagesToRead = messagesToRead;
    this.uniqueId = Writer.USER_ID + "-" + threadId;
  }

  @Override
  public void run() {
    Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
        .withQueryOptions(
            new QueryOptions()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.SERIAL))
        .build();
    Session session = cluster.connect("devoxx");

    // Créer le prepared statement pour la lecture depuis devoxx.messages_good

    // Créer le prepared statement pour écrire dans devoxx.messages_ack (UPDATE)

    // Créer le prepared statement pour supprimer l'enregistrement de devoxx.messages_good

    // Créer le prepared statement pour "locker" pendant 10s un enregistrement de devoxx.messages_good, si : processed_by = 'nobody'

    List<ResultSetFuture> readFutures = Lists.newArrayList();
    int i = 0;
    while (i <= messagesToRead) {
      boolean processed = false;

      // On utilise un bucketing par minute
      long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());

      for (int idQueue = 0; idQueue < 10; idQueue++) {
        // Lire depuis messages_good sur les deux dernières minutes et les 10 partitions de queues
        // mettre les futures dans readFutures
      }

      ImmutableList<ListenableFuture<ResultSet>> results = Futures.inCompletionOrder(readFutures);

      for (ListenableFuture<ResultSet> result : results) {
        ResultSet resultSet;
        try {
          resultSet = result.get();
          toploop: for (Row row : resultSet) {
            if (row.getString("processed_by").equals("nobody")) {
              // on tente de "locker" l'enregistrement pour éviter que d'autres ne le traitent

              if (lwtResult.wasApplied()) { // On vérifie que la LWT a réussi
                // lock réussi
                // Ecrire l'acquittement dans messages_ack
                // Supprimer l'enregistrement dans messages_good
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
