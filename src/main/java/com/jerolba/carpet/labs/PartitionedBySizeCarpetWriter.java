package com.jerolba.carpet.labs;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

class PartitionedBySizeCarpetWriter<T> implements PartitionWriter<T> {

    private final Class<T> recordClass;
    private final Iterator<String> fileNameGenerator;
    private final OutputFileFunction outputFileFunction;
    private final long maxFileSize;

    private CarpetSimpleWriter<T> currentWriter;

    public PartitionedBySizeCarpetWriter(Class<T> recordClass, PartitionedBySizeOutput config)
            throws IOException {
        this.recordClass = recordClass;
        this.maxFileSize = config.maxFileSize();
        this.fileNameGenerator = config.fileNameGenerator().newInstance();
        this.outputFileFunction = config.outputFileFunction();
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
        return currentWriter.writtenBytes() >= maxFileSize;
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
