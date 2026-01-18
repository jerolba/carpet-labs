package com.jerolba.carpet.labs;

import java.io.IOException;
import java.util.Collection;

interface PartitionWriter<T> {

    void write(T item) throws IOException;

    void write(Collection<T> records) throws IOException;

    void close() throws IOException;

}
