package com.sephora.services.ismsearchpoc.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ProcessingUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int MAX_CONTENT_LENGTH = 10000;

    public static String safeToString(Object obj) {
        if (obj == null) return "null";
        String str = String.valueOf(obj);
        return str.length() > MAX_CONTENT_LENGTH ?
                str.substring(0, MAX_CONTENT_LENGTH) + "... [TRUNCATED]" : str;
    }

    public static String objectToJson(Object obj) {
        if (obj == null) return "null";
        if (obj instanceof String) return (String) obj;
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            return obj.toString();
        }
    }

    public static Map<String, String> extractKafkaHeaders(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header ->
                headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8)));
        return headers;
    }

    public static String createProcessingIdentifier(ProcessingSource source, String... parts) {
        StringBuilder identifier = new StringBuilder(source.getValue());
        for (String part : parts) {
            if (part != null && !part.trim().isEmpty()) {
                identifier.append("-").append(part);
            }
        }
        return identifier.toString();
    }
}