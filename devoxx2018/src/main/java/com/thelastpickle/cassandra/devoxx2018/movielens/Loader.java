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
  public static void main(String[] args) {

    Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .build();
    Session session = cluster.connect("movielens");

    int nbFilms = 0;

    // Préparer la requête d'insertion

    ResultSet movies = null; // Lire depuis la table movies

    for (Row movie:movies) {
      // écrire en asynchrone dans la table movies_by_first_letter
    }

    System.out.println(nbFilms + " films traités...");
    cluster.close();
  }

}
