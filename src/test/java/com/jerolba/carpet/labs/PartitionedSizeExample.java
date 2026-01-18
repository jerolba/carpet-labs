package com.jerolba.carpet.labs;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import com.jerolba.carpet.io.FileSystemOutputFile;

public class PartitionedSizeExample {

    record Sample(int id, String name, String value) {
    }

    public static void main(String[] args) throws IOException {

        PartitionedBySizeOutput bySizeOutput = CarpetOutputBuilder
                .partitionedBySize(1024 * 1024 * 128) // 128 MB
                .withOutputFileFunction(path -> new FileSystemOutputFile(new File("/tmp/partition/" + path)))
                .build();

        Random rnd = new Random(0);
        try (PartitionedWriter<Sample> writer = new PartitionedWriter.Builder<>(Sample.class)
                .withRowGroupRowCountLimit(5_000_000)
                .withOutput(bySizeOutput)
                .build()) {
            for (int i = 0; i < 123_456_789; i++) {
                writer.write(new Sample(i, Integer.toString(rnd.nextInt(1000)), Integer.toString(rnd.nextInt(10000))));
            }
        }

    }
}
