package com.jerolba.carpet.labs;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

class PartitionedByNumberRecordsCarpetWriter<T> implements PartitionWriter<T> {

    private final Class<T> recordClass;
    private final Iterator<String> fileNameGenerator;
    private final OutputFileFunction outputFileFunction;
    private final long maxRecordsPerFile;

    private CarpetSimpleWriter<T> currentWriter;

    public PartitionedByNumberRecordsCarpetWriter(Class<T> recordClass,
            PartitionedByNumberRecordsOutput config)
            throws IOException {
        this.recordClass = recordClass;
        this.fileNameGenerator = config.fileNameGenerator().newInstance();
        this.outputFileFunction = config.outputFileFunction();
        this.maxRecordsPerFile = config.maxRecordsPerFile();
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
        return currentWriter.writtenRecords() >= maxRecordsPerFile;
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
        return new CarpetSimpleWriter<>(recordClass, outputFileFunction.buildOutputFile(fileName));
    }

}
