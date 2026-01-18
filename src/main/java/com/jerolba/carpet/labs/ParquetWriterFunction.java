package com.jerolba.carpet.labs;

import java.io.IOException;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

@FunctionalInterface
public interface ParquetWriterFunction<T> {

    ParquetWriter<T> buildParquetWriter(OutputFile outputFile) throws IOException;

}
