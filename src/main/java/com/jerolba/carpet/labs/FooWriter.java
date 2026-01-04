package com.jerolba.carpet.labs;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class FooWriter<T> implements Closeable, Consumer<T> {

    private final PartitionWriter<T> internalWriter;

    public FooWriter(Class<T> entityClass, CarpetOutput output) throws IOException {
        this.internalWriter = buildWriter(entityClass, output);
    }

    private PartitionWriter<T> buildWriter(Class<T> entityClass, CarpetOutput output) throws IOException {
        if (output instanceof PartitionedBySizeOutput bySize) {
            return new PartitionedBySizeCarpetWriter<>(entityClass, bySize);
        }
        if (output instanceof PartitionedByNumberRecordsOutput byNumberRecords) {
            return new PartitionedByNumberRecordsCarpetWriter<>(entityClass, byNumberRecords);
        }
        if (output instanceof PartitionedByValueOutput byValue) {
            return new PartitionedByValueCarpetWriter<T>(entityClass, byValue);
        }
        throw new IllegalArgumentException("Unsupported CarpetOutput configuration: " + output.getClass());
    }

    public void write(Collection<T> collection) throws IOException {
        for (var value : collection) {
            internalWriter.write(value);
        }
    }

    public void write(T value) throws IOException {
        internalWriter.write(value);
    }

    @Override
    public void accept(T value) {
        try {
            internalWriter.write(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void write(Stream<T> stream) throws IOException {
        Iterator<T> it = stream.iterator();
        while (it.hasNext()) {
            internalWriter.write(it.next());
        }
    }

    @Override
    public void close() throws IOException {
        internalWriter.close();
    }

}
