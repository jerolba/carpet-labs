package com.jerolba.carpet.labs;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionedByRowCountCarpetWriter<T> implements PartitionWriter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedByRowCountCarpetWriter.class);

    private final ParquetWriterFunction<T> writerFunction;
    private final Iterator<String> fileNameGenerator;
    private final CreatedFilesHolder createdFilesHolder;
    private final long maxRowsPerFile;

    private CarpetSimpleWriter<T> currentWriter;

    public PartitionedByRowCountCarpetWriter(ParquetWriterFunction<T> writerFunction,
            PartitionedByRowCountOutput config)
            throws IOException {
        this.writerFunction = writerFunction;
        this.fileNameGenerator = config.fileNameGenerator().newInstance();
        this.createdFilesHolder = new CreatedFilesHolder(config.outputFileFunction());
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
        LOGGER.info("Closing writer for latest file after {} records", currentWriter.writtenRecords());
        currentWriter.close();
    }

    private void rotate() throws IOException {
        LOGGER.info("Rotating file after {} records", currentWriter.writtenRecords());
        currentWriter.close();
        currentWriter = createWriter();
    }

    private CarpetSimpleWriter<T> createWriter() throws IOException {
        String fileName = fileNameGenerator.next();
        LOGGER.info("Creating new partitioned file with name: {}", fileName);
        OutputFile outputFile = createdFilesHolder.buildOutputFile(fileName);
        LOGGER.info("Output file: {}", outputFile.getPath());
        ParquetWriter<T> parquetWriter = writerFunction.buildParquetWriter(outputFile);
        return new CarpetSimpleWriter<>(parquetWriter);
    }

}
