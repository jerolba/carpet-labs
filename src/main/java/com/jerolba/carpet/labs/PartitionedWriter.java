package com.jerolba.carpet.labs;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.jerolba.carpet.CarpetWriterConfigurationBuilder;

public class PartitionedWriter<T> implements Closeable, Consumer<T> {

    private final PartitionWriter<T> writer;

    public PartitionedWriter(CarpetOutput output, Class<T> recordClass) throws IOException {
        this(output, new Builder<>(recordClass).parquetWriterFunction());
    }

    private PartitionedWriter(CarpetOutput output, ParquetWriterFunction<T> writerFunction) throws IOException {
        this.writer = buildWriter(writerFunction, output);
    }

    private PartitionWriter<T> buildWriter(ParquetWriterFunction<T> writerFunction, CarpetOutput output)
            throws IOException {
        if (output instanceof PartitionedBySizeOutput bySize) {
            return new PartitionedBySizeCarpetWriter<>(writerFunction, bySize);
        }
        if (output instanceof PartitionedByRowCountOutput byNumberRecords) {
            return new PartitionedByRowCountCarpetWriter<>(writerFunction, byNumberRecords);
        }
        if (output instanceof PartitionedByValueOutput byValue) {
            return new PartitionedByValueCarpetWriter<T>(writerFunction, byValue);
        }
        throw new IllegalArgumentException("Unsupported CarpetOutput configuration: " + output.getClass());
    }

    /**
     *
     * Writes the specified collection of Java objects to a Parquet file.
     *
     * @param collection the collection of objects to write
     * @throws IOException if an error occurs while writing the records
     */
    public void write(Collection<T> collection) throws IOException {
        for (var value : collection) {
            writer.write(value);
        }
    }

    /**
     *
     * Writes the specified Java object to a Parquet file
     *
     * @param value object to write
     * @throws IOException if an error occurs while writing the records
     */
    public void write(T value) throws IOException {
        writer.write(value);
    }

    /**
     *
     * Writes the specified Java object to a Parquet file implementing Consumer<T>
     *
     * @param value object to write
     * @throws UncheckedIOException if an error occurs while writing the records
     */
    @Override
    public void accept(T value) {
        try {
            writer.write(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     *
     * Writes the specified stream of Java objects to a Parquet file.
     *
     * @param stream the stream of objects to write
     *
     * @throws IOException if an error occurs while writing the records
     */
    public void write(Stream<T> stream) throws IOException {
        Iterator<T> it = stream.iterator();
        while (it.hasNext()) {
            writer.write(it.next());
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    public static class Builder<T> extends CarpetWriterConfigurationBuilder<T, Builder<T>> {

        private CarpetOutput output;

        public Builder(Class<T> recordClass) {
            super(recordClass);
        }

        public Builder<T> withOutput(CarpetOutput output) {
            this.output = output;
            return this;
        }

        @Override
        protected Builder<T> self() {
            return this;
        }

        public PartitionedWriter<T> build() throws IOException {
            return new PartitionedWriter<>(output, parquetWriterFunction());
        }

        private ParquetWriterFunction<T> parquetWriterFunction() {
            return outputFile -> super.withFile(outputFile).buildParquetWriter();
        }

    }

}
