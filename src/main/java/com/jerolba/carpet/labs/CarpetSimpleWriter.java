package com.jerolba.carpet.labs;

import java.io.IOException;

import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import com.jerolba.carpet.CarpetParquetWriter;

class CarpetSimpleWriter<T> {

    private final ParquetWriter<T> parquetWriter;
    private long recordCount;

    public CarpetSimpleWriter(Class<T> clazz, OutputFile outputFile) throws IOException {
        this.parquetWriter = CarpetParquetWriter.builder(outputFile, clazz)
                .withWriteMode(Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
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