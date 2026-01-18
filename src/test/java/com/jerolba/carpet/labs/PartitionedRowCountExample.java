package com.jerolba.carpet.labs;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import com.jerolba.carpet.io.FileSystemOutputFile;

public class PartitionedRowCountExample {

    record Sample(int id, String name, String value) {
    }

    public static void main(String[] args) throws IOException {
        PartitionedByRowCountOutput byRowCountOutput = CarpetOutputBuilder
                .partitionedByRowCount(14_247_939L)
                .withOutputFileFunction(path -> new FileSystemOutputFile(new File("/tmp/partition/" + path)))
                .build();

        Random rnd = new Random(0);
        try (PartitionedWriter<Sample> writer = new PartitionedWriter<>(byRowCountOutput, Sample.class)) {
            for (int i = 0; i < 123_456__789; i++) {
                writer.write(new Sample(i, Integer.toString(rnd.nextInt(1000)), Integer.toString(rnd.nextInt(10000))));
            }
        }
    }

}
