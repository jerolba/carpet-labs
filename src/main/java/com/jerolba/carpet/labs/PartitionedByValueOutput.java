package com.jerolba.carpet.labs;

import java.util.List;

public class PartitionedByValueOutput<T> implements CarpetOutput {

    private final List<PartitionRule<T>> partitionRules;
    private final SimpleFileNameGenerator fileNameGenerator;
    private final OutputFileFunction outputFileFunction;
    private final int maxOpenFiles;

    public PartitionedByValueOutput(List<PartitionRule<T>> partitionRules,
            SimpleFileNameGenerator fileNameGenerator, OutputFileFunction outputFileFunction, int maxOpenFiles) {
        this.fileNameGenerator = fileNameGenerator;
        this.outputFileFunction = outputFileFunction;
        this.partitionRules = partitionRules;
        this.maxOpenFiles = maxOpenFiles;
    }

    public SimpleFileNameGenerator fileNameGenerator() {
        return fileNameGenerator;
    }

    public OutputFileFunction outputFileFunction() {
        return outputFileFunction;
    }

    public List<PartitionRule<T>> partitionRules() {
        return partitionRules;
    }

    public int maxOpenFiles() {
        return maxOpenFiles;
    }

}
