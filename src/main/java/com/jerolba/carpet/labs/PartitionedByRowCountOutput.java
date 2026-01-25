package com.jerolba.carpet.labs;

import java.util.List;

public class PartitionedByRowCountOutput implements CarpetOutput {

    private final SimpleFileNameGenerator fileNameGenerator;
    private final CreatedFilesHolder createdFilesHolder;
    private final long maxRowsPerFile;

    public PartitionedByRowCountOutput(long maxRowsPerFile,
            SimpleFileNameGenerator fileNameGenerator, OutputFileFunction outputFileFunction) {
        this.fileNameGenerator = fileNameGenerator;
        this.createdFilesHolder = new CreatedFilesHolder(outputFileFunction);
        this.maxRowsPerFile = maxRowsPerFile;
    }

    public SimpleFileNameGenerator fileNameGenerator() {
        return fileNameGenerator;
    }

    public OutputFileFunction outputFileFunction() {
        return createdFilesHolder;
    }

    public long maxRowsPerFile() {
        return maxRowsPerFile;
    }

    @Override
    public List<String> getCreatedFiles() {
        return createdFilesHolder.getCreatedFiles();
    }

}
