package com.jerolba.carpet.labs;

import java.util.List;

public class PartitionedBySizeOutput implements CarpetOutput {

    private final long maxFileSize;
    private final SimpleFileNameGenerator fileNameGenerator;
    private final CreatedFilesHolder createdFilesHolder;

    public PartitionedBySizeOutput(long maxFileSize, SimpleFileNameGenerator fileNameGenerator,
            OutputFileFunction outputFileFunction) {
        this.fileNameGenerator = fileNameGenerator;
        this.createdFilesHolder = new CreatedFilesHolder(outputFileFunction);
        this.maxFileSize = maxFileSize;
    }

    public SimpleFileNameGenerator fileNameGenerator() {
        return fileNameGenerator;
    }

    public OutputFileFunction outputFileFunction() {
        return createdFilesHolder;
    }

    public long maxFileSize() {
        return maxFileSize;
    }

    @Override
    public List<String> getCreatedFiles() {
        return createdFilesHolder.getCreatedFiles();
    }

}
