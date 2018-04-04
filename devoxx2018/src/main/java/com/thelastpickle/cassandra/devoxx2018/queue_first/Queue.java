package com.thelastpickle.cassandra.devoxx2018.queue_first;

public class Queue {
  public static void main(String[] args) throws InterruptedException {
    int messagesToProcess = Integer.parseInt(args[1]);
    if (args[0].equals("write")) {
      Writer.write(messagesToProcess, Integer.parseInt(args[2]));
    }
    else {
      Reader.read(messagesToProcess);
    }
  }
}
