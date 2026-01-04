package com.jerolba.carpet.labs;

public class PartitionedByNumberRecordsOutput implements CarpetOutput {

    private final SimpleFileNameGenerator fileNameGenerator;
    private final OutputFileFunction outputFileFunction;
    private final long maxRecordsPerFile;

    public PartitionedByNumberRecordsOutput(long maxRecordsPerFile,
            SimpleFileNameGenerator fileNameGenerator, OutputFileFunction outputFileFunction) {
        this.fileNameGenerator = fileNameGenerator;
        this.outputFileFunction = outputFileFunction;
        this.maxRecordsPerFile = maxRecordsPerFile;
    }

    public SimpleFileNameGenerator fileNameGenerator() {
        return fileNameGenerator;
    }

    public OutputFileFunction outputFileFunction() {
        return outputFileFunction;
    }

    public long maxRecordsPerFile() {
        return maxRecordsPerFile;
    }

}
