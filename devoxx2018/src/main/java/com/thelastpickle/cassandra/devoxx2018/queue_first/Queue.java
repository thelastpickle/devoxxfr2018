package com.thelastpickle.cassandra.devoxx2018.queue_first;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

public class Queue {
  public static void main(String[] args) throws InterruptedException {
    int messagesToProcess = Integer.parseInt(args[1]);
    if (args[0].equals("write")) {
      Writer.write(messagesToProcess, Integer.parseInt(args[2]));
    }
    else {
      int nbThreads = Integer.parseInt(args[2]);
      ExecutorService executor = Executors.newFixedThreadPool(nbThreads);
      List<Thread> threads = Lists.newArrayList();
      for (int i = 0; i < nbThreads; i++) {
        threads.add(new Thread(new Reader(messagesToProcess)));
      }

      threads.stream().forEach(thread -> executor.submit(thread));
      executor.awaitTermination(1, TimeUnit.HOURS);

    }
  }
}
