package com.jerolba.carpet.labs;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

class PartitionSpec<T> {

    private final String keyName;
    private final Function<T, String> extractor;
    private final Map<String, PartitionValue> encodedValues = new HashMap<>();

    public PartitionSpec(String keyName, Function<T, String> extractor) {
        this.keyName = keyName;
        this.extractor = extractor;
    }

    public String apply(T record) {
        String value = extractor.apply(record);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Partition value cannot be null or empty for key: " + keyName);
        }
        PartitionValue pv = encodedValues.computeIfAbsent(value,
                v -> new PartitionValue(keyName, value, keyName + "=" + PartitionValueEncoder.encode(v) + "/"));
        return pv.partitionValue();
    }

    private record PartitionValue(String keyName, String rawValue, String partitionValue) {
    }

}