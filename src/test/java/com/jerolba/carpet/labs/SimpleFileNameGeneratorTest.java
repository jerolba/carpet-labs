package com.jerolba.carpet.labs;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.jerolba.carpet.labs.SimpleFileNameGenerator;

public class SimpleFileNameGeneratorTest {

    @Test
    void sequenceIncrementedAndPlaceholdersApplied() {
        Iterator<String> generator = new SimpleFileNameGenerator("file-{seq}.txt").newInstance();

        assertEquals("file-0.txt", generator.next());
        assertEquals("file-1.txt", generator.next());
        assertEquals("file-2.txt", generator.next());
    }

    @Test
    void indexIncrementedAndPlaceholdersApplied() {
        Iterator<String> generator = new SimpleFileNameGenerator("file-{i}.txt").newInstance();

        assertEquals("file-0.txt", generator.next());
        assertEquals("file-1.txt", generator.next());
        assertEquals("file-2.txt", generator.next());
    }

    @Test
    void uuidTemplateProducesValidUniqueValues() {
        Iterator<String> generator = new SimpleFileNameGenerator("file-{uuid}.json").newInstance();

        String first = generator.next();
        String second = generator.next();
        assertTrue(first.startsWith("file-") && first.endsWith(".json"));
        assertDoesNotThrow(() -> UUID.fromString(first.replace("file-", "").replace(".json", "")));
        assertTrue(second.startsWith("file-") && second.endsWith(".json"));
        assertDoesNotThrow(() -> UUID.fromString(second.replace("file-", "").replace(".json", "")));
        assertNotEquals(first, second);
    }

    @Test
    void missingPlaceholderThrows() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new SimpleFileNameGenerator("file.txt"));

        assertTrue(exception.getMessage().contains("{seq}") || exception.getMessage().contains("{uuid}"));
    }
}