package com.jerolba.carpet.labs;

class PartitionValueEncoder {

    public static String encode(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Partition value cannot be null or empty");
        }

        StringBuilder encoded = new StringBuilder();
        for (char c : value.toCharArray()) {
            if (Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.') {
                encoded.append(c);
            } else if (c == '%') {
                encoded.append("%25");
            } else if (c == '=') {
                encoded.append("%3D");
            } else if (c == '/') {
                encoded.append("%2F");
            } else if (c == ' ') {
                encoded.append("%20");
            } else {
                // Encode other special characters
                encoded.append(String.format("%%%02X", (int) c));
            }
        }
        return encoded.toString();
    }

}