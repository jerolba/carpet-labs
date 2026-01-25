package com.jerolba.carpet.labs;

import org.apache.parquet.io.OutputFile;

@FunctionalInterface
public interface OutputFileFunction {

    /**
     * Given a composed file path, builds an OutputFile
     *
     * @param filePath
     * @return OutputFile
     */
    OutputFile buildOutputFile(String filePath);

}
