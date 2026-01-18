package com.jerolba.carpet.labs;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Random;

import com.jerolba.carpet.io.FileSystemOutputFile;

public class PartitionedByValueExample {

    record RowData(int id, String name, LocalDate date) {
    }

    public static void main(String[] args) throws IOException {
        PartitionedByValueOutput<RowData> byValueOutput = CarpetOutputBuilder
                .partitionedByValue(RowData.class)
                .withRule("year", rowData -> Integer.toString(rowData.date().getYear()))
                .withRule("month", rowData -> Integer.toString(rowData.date().getMonthValue()))
                .withMaxOpenFiles(100)
                .withOutputFileFunction(path -> {
                    File file = new File("/tmp/partition/" + path);
                    file.getParentFile().mkdirs();
                    return new FileSystemOutputFile(file);
                })
                .build();

        Random rnd = new Random(0);
        try (PartitionedWriter<RowData> writer = new PartitionedWriter.Builder<>(RowData.class)
                .withOutput(byValueOutput)
                .build()) {
            for (int i = 0; i < 123_456_789; i++) {
                int year = 2020 + rnd.nextInt(2);
                int month = 1 + rnd.nextInt(12);
                LocalDate date = LocalDate.of(year, month, 1 + rnd.nextInt(28));
                writer.write(new RowData(i, Integer.toString(rnd.nextInt(1000)), date));
            }
        }

    }
}
