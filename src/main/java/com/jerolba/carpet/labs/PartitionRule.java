package com.jerolba.carpet.labs;

import java.util.function.Function;

public record PartitionRule<T>(String keyName, Function<T, String> extractor) {

}
