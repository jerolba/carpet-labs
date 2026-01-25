package com.jerolba.carpet.labs;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionedBySizeCarpetWriter<T> implements PartitionWriter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedBySizeCarpetWriter.class);

    private final ParquetWriterFunction<T> writerFunction;
    private final Iterator<String> fileNameGenerator;
    private final OutputFileFunction outputFileFunction;
    private final long maxFileSize;

    private CarpetSimpleWriter<T> currentWriter;

    private long recordRate = 128;

    public PartitionedBySizeCarpetWriter(ParquetWriterFunction<T> writerFunction, PartitionedBySizeOutput config)
            throws IOException {
        this.writerFunction = writerFunction;
        this.maxFileSize = config.maxFileSize();
        this.fileNameGenerator = config.fileNameGenerator().newInstance();
        this.outputFileFunction = config.outputFileFunction();
        createWriter();
    }

    @Override
    public void write(T item) throws IOException {
        if (needsRotation()) {
            rotate();
        }
        currentWriter.write(item);
    }

    private boolean needsRotation() throws IOException {
        long writtenRecords = currentWriter.writtenRecords();
        if (writtenRecords % recordRate == 0) {
            long writtenBytes = currentWriter.writtenBytes();
            if (writtenBytes > maxFileSize) {
                return true;
            }

            double percentWritten = (double) writtenBytes / maxFileSize * 100;
            double averageBytesPerRecord = (double) writtenBytes / writtenRecords;
            double estimatedNumberRecords = maxFileSize / averageBytesPerRecord;
            long tenPercentRecords = (long) (estimatedNumberRecords * 0.1);

            if (percentWritten > 99.0) {
                recordRate = 100;
            } else if (percentWritten > 80.0) {
                recordRate = (long) (estimatedNumberRecords * 0.01);
            } else if (percentWritten < 20.0 && tenPercentRecords > 0) {
                recordRate = Math.min(tenPercentRecords, recordRate * 2);
            }
        }
        return false;
    }

    @Override
    public void write(Collection<T> records) throws IOException {
        for (T record : records) {
            write(record);
        }
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing writer for latest file after aprox {} bytes written", currentWriter.writtenBytes());
        currentWriter.close();
    }

    private void rotate() throws IOException {
        LOGGER.info("Rotating file after aprox {} bytes written", currentWriter.writtenBytes());
        currentWriter.close();
        createWriter();
    }

    private void createWriter() throws IOException {
        String fileName = fileNameGenerator.next();
        LOGGER.info("Creating new partitioned file with name: {}", fileName);
        OutputFile outputFile = outputFileFunction.buildOutputFile(fileName);
        LOGGER.info("Output file: {}", outputFile.getPath());
        this.currentWriter = new CarpetSimpleWriter<>(writerFunction.buildParquetWriter(outputFile));
    }

}
