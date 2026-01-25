package com.jerolba.carpet.labs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.parquet.io.OutputFile;

public class CreatedFilesHolder implements OutputFileFunction {

    private final List<String> createdFiles = new ArrayList<>();
    private final OutputFileFunction outputFileFunction;

    public CreatedFilesHolder(OutputFileFunction outputFileFunction) {
        this.outputFileFunction = outputFileFunction;
    }

    public List<String> getCreatedFiles() {
        return Collections.unmodifiableList(createdFiles);
    }

    @Override
    public OutputFile buildOutputFile(String filePath) {
        OutputFile outputFile = outputFileFunction.buildOutputFile(filePath);
        String path = outputFile.getPath();
        createdFiles.add(path);
        return outputFile;

    }

}
