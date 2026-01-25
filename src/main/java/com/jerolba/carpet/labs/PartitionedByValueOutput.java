package com.jerolba.carpet.labs;

import java.util.List;

public class PartitionedByValueOutput<T> implements CarpetOutput {

    private final List<PartitionRule<T>> partitionRules;
    private final SimpleFileNameGenerator fileNameGenerator;
    private final CreatedFilesHolder createdFilesHolder;
    private final int maxOpenFiles;

    public PartitionedByValueOutput(List<PartitionRule<T>> partitionRules,
            SimpleFileNameGenerator fileNameGenerator, OutputFileFunction outputFileFunction, int maxOpenFiles) {
        this.fileNameGenerator = fileNameGenerator;
        this.createdFilesHolder = new CreatedFilesHolder(outputFileFunction);
        this.partitionRules = partitionRules;
        this.maxOpenFiles = maxOpenFiles;
    }

    public SimpleFileNameGenerator fileNameGenerator() {
        return fileNameGenerator;
    }

    public OutputFileFunction outputFileFunction() {
        return createdFilesHolder;
    }

    public List<PartitionRule<T>> partitionRules() {
        return partitionRules;
    }

    public int maxOpenFiles() {
        return maxOpenFiles;
    }

    @Override
    public List<String> getCreatedFiles() {
        return createdFilesHolder.getCreatedFiles();
    }

}
