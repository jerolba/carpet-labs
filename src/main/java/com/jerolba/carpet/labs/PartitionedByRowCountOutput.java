package com.jerolba.carpet.labs;

public class PartitionedByRowCountOutput implements CarpetOutput {

    private final SimpleFileNameGenerator fileNameGenerator;
    private final OutputFileFunction outputFileFunction;
    private final long maxRowsPerFile;

    public PartitionedByRowCountOutput(long maxRowsPerFile,
            SimpleFileNameGenerator fileNameGenerator, OutputFileFunction outputFileFunction) {
        this.fileNameGenerator = fileNameGenerator;
        this.outputFileFunction = outputFileFunction;
        this.maxRowsPerFile = maxRowsPerFile;
    }

    public SimpleFileNameGenerator fileNameGenerator() {
        return fileNameGenerator;
    }

    public OutputFileFunction outputFileFunction() {
        return outputFileFunction;
    }

    public long maxRowsPerFile() {
        return maxRowsPerFile;
    }

}
