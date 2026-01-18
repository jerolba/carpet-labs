package com.jerolba.carpet.labs;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

class PartitionedByValueCarpetWriter<T> implements PartitionWriter<T> {

    private final PartitionPathBuilder<T> partitionPathBuilder;
    private final PartitionWriterRegistry<T> partitionWriterRegistry;

    public PartitionedByValueCarpetWriter(ParquetWriterFunction<T> writerFunction, PartitionedByValueOutput<T> config) {
        this.partitionPathBuilder = new PartitionPathBuilder<>(config.partitionRules());
        this.partitionWriterRegistry = new PartitionWriterRegistry<>(
                config.maxOpenFiles(),
                writerFunction,
                config.outputFileFunction(),
                config.fileNameGenerator());
    }

    @Override
    public void write(T item) throws IOException {
        String partitionPath = partitionPathBuilder.buildPartitionPath(item);
        CarpetSimpleWriter<T> writer = partitionWriterRegistry.getOrCreate(partitionPath);
        writer.write(item);
    }

    @Override
    public void write(Collection<T> records) throws IOException {
        for (T record : records) {
            write(record);
        }
    }

    @Override
    public void close() throws IOException {
        partitionWriterRegistry.closeAll();

    }

    private static class PartitionPathBuilder<T> {

        private final List<PartitionSpec<T>> specs;

        public PartitionPathBuilder(List<PartitionRule<T>> rules) {
            this.specs = rules.stream().map(rule -> new PartitionSpec<>(rule.keyName(), rule.extractor())).toList();
        }

        public String buildPartitionPath(T record) {
            if (specs.size() == 1) {
                return specs.get(0).apply(record);
            }
            StringBuilder path = new StringBuilder();
            for (PartitionSpec<T> spec : specs) {
                path.append(spec.apply(record));
            }
            return path.toString();
        }

    }

    /**
     * A simple counter to track order of access for LRU eviction.
     */
    private static class Counter {

        private long count = 0;

        public long next() {
            return count++;
        }

    }

    private static class PartitionWriterRegistry<T> {

        private final int maxOpenFiles;
        private final ParquetWriterFunction<T> writerFunction;
        private final OutputFileFunction outputFileFunction;
        private final SimpleFileNameGenerator simpleFileNameGenerator;
        private final Map<String, PartitionWriterEntry<T>> activeWriters;
        private final Map<String, PartitionWriterEntry<T>> evictedWriters;
        private final Counter counter = new Counter();

        PartitionWriterRegistry(int maxOpenFiles, ParquetWriterFunction<T> writerFunction,
                OutputFileFunction outputFileFunction,
                SimpleFileNameGenerator simpleFileNameGenerator) {
            this.maxOpenFiles = maxOpenFiles;
            this.writerFunction = writerFunction;
            this.outputFileFunction = outputFileFunction;
            this.simpleFileNameGenerator = simpleFileNameGenerator;
            this.activeWriters = new HashMap<>(maxOpenFiles);
            this.evictedWriters = new HashMap<>();
        }

        CarpetSimpleWriter<T> getOrCreate(String partitionPath) throws IOException {
            PartitionWriterEntry<T> entry = activeWriters.get(partitionPath);
            if (entry != null) {
                return entry.getWriter();
            }
            if (activeWriters.size() >= maxOpenFiles) {
                evictLRU();
            }
            PartitionWriterEntry<T> nonActive = evictedWriters.computeIfAbsent(partitionPath,
                    path -> new PartitionWriterEntry<>(writerFunction, partitionPath, outputFileFunction,
                            simpleFileNameGenerator, counter));
            activeWriters.put(partitionPath, nonActive);
            nonActive.createNewWriter();
            return nonActive.getWriter();
        }

        public void closeAll() throws IOException {
            Exception firstException = null;
            for (PartitionWriterEntry<T> entry : activeWriters.values()) {
                try {
                    entry.close();
                } catch (Exception e) {
                    if (firstException == null) {
                        firstException = e;
                    }
                }
            }
            activeWriters.clear();
            evictedWriters.clear();
            if (firstException != null) {
                if (firstException instanceof IOException) {
                    throw (IOException) firstException;
                } else {
                    throw new IOException("Error closing writers", firstException);
                }
            }
        }

        private void evictLRU() throws IOException {
            Set<Entry<String, PartitionWriterEntry<T>>> entrySet = activeWriters.entrySet();
            Entry<String, PartitionWriterEntry<T>> leastRecentlyUsed = null;
            long oldestAccessTime = Long.MAX_VALUE;
            for (var entry : entrySet) {
                if (entry.getValue().lastAccessTime < oldestAccessTime) {
                    oldestAccessTime = entry.getValue().lastAccessTime;
                    leastRecentlyUsed = entry;
                }
            }
            leastRecentlyUsed.getValue().close();
            activeWriters.remove(leastRecentlyUsed.getKey());
        }

        static class PartitionWriterEntry<T> {

            private final ParquetWriterFunction<T> writerFunction;
            private final String partitionPath;
            private final OutputFileFunction outputFileFunction;
            private final Iterator<String> nameIterator;
            private final Counter counter;
            private CarpetSimpleWriter<T> writer;
            private long lastAccessTime;

            PartitionWriterEntry(ParquetWriterFunction<T> writerFunction, String partitionPath,
                    OutputFileFunction outputFileFunction,
                    SimpleFileNameGenerator simpleFileNameGenerator, Counter counter) {
                this.writerFunction = writerFunction;
                this.partitionPath = partitionPath;
                this.outputFileFunction = outputFileFunction;
                this.nameIterator = simpleFileNameGenerator.newInstance();
                this.counter = counter;
            }

            void createNewWriter() throws IOException {
                String fileName = nameIterator.next();
                OutputFile outputFile = outputFileFunction.buildOutputFile(partitionPath + fileName);
                ParquetWriter<T> parquetWriter = writerFunction.buildParquetWriter(outputFile);
                writer = new CarpetSimpleWriter<>(parquetWriter);
            }

            CarpetSimpleWriter<T> getWriter() {
                this.lastAccessTime = counter.next();
                return writer;
            }

            void close() throws IOException {
                if (writer != null) {
                    writer.close();
                }
            }

        }
    }

}
