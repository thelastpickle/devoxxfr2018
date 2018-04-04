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
    // Créer sa connexion au cluster et son objet Session

    // Créer un prepared statement pour la lecture dans devoxx.messages

    // Créer un prepared statement pour l'écriture dans devoxx.messages_ack

    // Créer un prepared statement pour la suppression des messages dans
    // devoxx.messages

    for (int i = 0; i <= messagesToRead; i++) {
      int idQueue = 1;

      // Lire le plus ancien message de la queue
      // Row row = ...
      if (row != null) {
        // S'il y en a un, écrire l'acquittement dans devoxx.messages_ack

        // Supprimer l'enregistrement de devoxx.messages

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

    // Fermer sa connexion au cluster

  }
}
