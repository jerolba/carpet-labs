package com.jerolba.carpet.labs;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

class PartitionedByRowCountCarpetWriter<T> implements PartitionWriter<T> {

    private final ParquetWriterFunction<T> writerFunction;
    private final Iterator<String> fileNameGenerator;
    private final OutputFileFunction outputFileFunction;
    private final long maxRowsPerFile;

    private CarpetSimpleWriter<T> currentWriter;

    public PartitionedByRowCountCarpetWriter(ParquetWriterFunction<T> writerFunction,
            PartitionedByRowCountOutput config)
            throws IOException {
        this.writerFunction = writerFunction;
        this.fileNameGenerator = config.fileNameGenerator().newInstance();
        this.outputFileFunction = config.outputFileFunction();
        this.maxRowsPerFile = config.maxRowsPerFile();
        this.currentWriter = createWriter();
    }

    @Override
    public void write(T item) throws IOException {
        if (needsRotation()) {
            rotate();
        }
        currentWriter.write(item);
    }

    private boolean needsRotation() {
        return currentWriter.writtenRecords() >= maxRowsPerFile;
    }

    @Override
    public void write(Collection<T> records) throws IOException {
        for (T record : records) {
            write(record);
        }
    }

    @Override
    public void close() throws IOException {
        currentWriter.close();
    }

    private void rotate() throws IOException {
        currentWriter.close();
        currentWriter = createWriter();
    }

    private CarpetSimpleWriter<T> createWriter() throws IOException {
        String fileName = fileNameGenerator.next();
        OutputFile outputFile = outputFileFunction.buildOutputFile(fileName);
        ParquetWriter<T> parquetWriter = writerFunction.buildParquetWriter(outputFile);
        return new CarpetSimpleWriter<>(parquetWriter);
    }

}
