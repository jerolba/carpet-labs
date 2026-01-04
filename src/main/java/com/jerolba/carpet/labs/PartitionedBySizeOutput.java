package com.jerolba.carpet.labs;

public class PartitionedBySizeOutput implements CarpetOutput {

    private final long maxFileSize;
    private final SimpleFileNameGenerator fileNameGenerator;
    private final OutputFileFunction outputFileFunction;

    public PartitionedBySizeOutput(long maxFileSize, SimpleFileNameGenerator fileNameGenerator,
            OutputFileFunction outputFileFunction) {
        this.fileNameGenerator = fileNameGenerator;
        this.outputFileFunction = outputFileFunction;
        this.maxFileSize = maxFileSize;
    }

    public SimpleFileNameGenerator fileNameGenerator() {
        return fileNameGenerator;
    }

    public OutputFileFunction outputFileFunction() {
        return outputFileFunction;
    }

    public long maxFileSize() {
        return maxFileSize;
    }

}
