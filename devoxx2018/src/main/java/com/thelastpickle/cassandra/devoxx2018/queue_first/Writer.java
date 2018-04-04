package com.thelastpickle.cassandra.devoxx2018.queue_first;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;



public final class Writer {
  public static final String USER_ID = "alex";

  public static void write(int messagesToInsert, int rateLimit) {
    RateLimiter rateLimiter = RateLimiter.create(rateLimit);

    // Créer sa connexion au cluster et son objet Session

    // Créer un prepared statement pour les écritures dans devoxx.messages

    for (int i = 0; i <= messagesToInsert; i++) {
      int idQueue = 1;
      // Insérer les enregistrements par lots de 100 requêtes asynchrones
      // en utilisant le prepared statement
      System.out.println("Insertions :  " + messagesToInsert + " enregistrements...");
    }

    System.out.println("Insertions : " + messagesToInsert + " enregistrements...");

    // Fermer sa connexion au cluster

  }
}
