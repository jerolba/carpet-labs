package com.jerolba.carpet.labs;

import java.util.Iterator;
import java.util.UUID;

class SimpleFileNameGenerator {

    private enum Criteria {
        SEQUENCE,
        UUID,
    }

    private final String template;
    private final Criteria criteria;

    public SimpleFileNameGenerator(String template) {
        this.template = withOutTailParquet(template);
        if (this.template.contains("{seq}") || this.template.contains("{i}")) {
            criteria = Criteria.SEQUENCE;
        } else if (this.template.contains("{uuid}")) {
            criteria = Criteria.UUID;
        } else {
            throw new IllegalArgumentException("Template must contain either {seq}, {i} or {uuid} placeholder");
        }
    }

    private static String withOutTailParquet(String template) {
        if (template.endsWith(".parquet")) {
            return template.substring(0, template.length() - ".parquet".length());
        }
        return template;
    }

    public Iterator<String> newInstance() {
        return new NameIterator();
    }

    public class NameIterator implements Iterator<String> {

        private int seq;

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            String name = switch (criteria) {
            case SEQUENCE -> generateSequenceName();
            case UUID -> generateUuidName();
            };
            return name + ".parquet";
        }

        private String generateUuidName() {
            return template.replace("{uuid}", UUID.randomUUID().toString());
        }

        private String generateSequenceName() {
            String valueOf = String.valueOf(seq++);
            return template.replace("{seq}", valueOf).replace("{i}", valueOf);
        }

    }

}