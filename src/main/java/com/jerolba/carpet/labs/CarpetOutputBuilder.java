package com.jerolba.carpet.labs;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class CarpetOutputBuilder {

    public static PartitionedBySizeBuilder partitionedBySize(long size) {
        return new PartitionedBySizeBuilder(size);
    }

    public static PartitionedByNumberRecordsBuilder partitionedByRowCount(long numberRecords) {
        return new PartitionedByNumberRecordsBuilder(numberRecords);
    }

    public static <T> PartitionedByValueBuilder<T> partitionedByValue(String keyName, Function<T, String> extractor) {
        return new PartitionedByValueBuilder<T>().withRule(keyName, extractor);
    }

    public static <T> PartitionedByValueBuilder<T> partitionedByValue(Class<T> type) {
        return new PartitionedByValueBuilder<>();
    }

    public static class PartitionedBySizeBuilder {

        private final long size;
        private String fileNameTemplate = "{i}.parquet";
        private OutputFileFunction outputFileFunction;

        private PartitionedBySizeBuilder(long size) {
            this.size = size;
        }

        public PartitionedBySizeBuilder withFileNameTemplate(String fileNameTemplate) {
            this.fileNameTemplate = fileNameTemplate;
            return this;
        }

        public PartitionedBySizeBuilder withOutputFileFunction(OutputFileFunction outputFileFunction) {
            this.outputFileFunction = outputFileFunction;
            return this;
        }

        public PartitionedBySizeOutput build() {
            return new PartitionedBySizeOutput(
                    size,
                    new SimpleFileNameGenerator(fileNameTemplate),
                    outputFileFunction);
        }

    }

    public static class PartitionedByNumberRecordsBuilder {

        private final long numberRecords;
        private String fileNameTemplate = "{i}.parquet";
        private OutputFileFunction outputFileFunction;

        private PartitionedByNumberRecordsBuilder(long numberRecords) {
            this.numberRecords = numberRecords;
        }

        public PartitionedByNumberRecordsBuilder withFileNameTemplate(String fileNameTemplate) {
            this.fileNameTemplate = fileNameTemplate;
            return this;
        }

        public PartitionedByNumberRecordsBuilder withOutputFileFunction(OutputFileFunction outputFileFunction) {
            this.outputFileFunction = outputFileFunction;
            return this;
        }

        public PartitionedByRowCountOutput build() {
            return new PartitionedByRowCountOutput(
                    numberRecords,
                    new SimpleFileNameGenerator(fileNameTemplate),
                    outputFileFunction);
        }

    }

    public static class PartitionedByValueBuilder<T> {

        private final List<PartitionRule<T>> rules = new ArrayList<>();
        private String fileNameTemplate = "{i}.parquet";
        private OutputFileFunction outputFileFunction;
        private int maxOpenFiles = 10;

        private PartitionedByValueBuilder() {
        }

        public PartitionedByValueBuilder<T> withRule(String keyName, Function<T, String> extractor) {
            this.rules.add(new PartitionRule<>(keyName, extractor));
            return this;
        }

        public PartitionedByValueBuilder<T> withFileNameTemplate(String fileNameTemplate) {
            this.fileNameTemplate = fileNameTemplate;
            return this;
        }

        public PartitionedByValueBuilder<T> withOutputFileFunction(OutputFileFunction outputFileFunction) {
            this.outputFileFunction = outputFileFunction;
            return this;
        }

        public PartitionedByValueBuilder<T> withMaxOpenFiles(int maxOpenFiles) {
            this.maxOpenFiles = maxOpenFiles;
            return this;
        }

        public PartitionedByValueOutput<T> build() {
            return new PartitionedByValueOutput<>(
                    rules,
                    new SimpleFileNameGenerator(fileNameTemplate),
                    outputFileFunction,
                    maxOpenFiles);
        }

    }

}