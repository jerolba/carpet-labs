package com.jerolba.carpet.labs;

import java.io.IOException;

import org.apache.parquet.hadoop.ParquetWriter;

class CarpetSimpleWriter<T> {

    private final ParquetWriter<T> parquetWriter;
    private long recordCount;

    public CarpetSimpleWriter(ParquetWriter<T> parquetWriter) throws IOException {
        this.parquetWriter = parquetWriter;
        this.recordCount = 0;
    }

    public void write(T record) throws IOException {
        parquetWriter.write(record);
        ++recordCount;
    }

    public long writtenBytes() {
        return parquetWriter.getDataSize();
    }

    public long writtenRecords() {
        return recordCount;
    }

    public void close() throws IOException {
        parquetWriter.close();
    }

}