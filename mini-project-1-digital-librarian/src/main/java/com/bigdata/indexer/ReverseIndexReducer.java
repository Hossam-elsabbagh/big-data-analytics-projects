package com.bigdata.indexer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReverseIndexReducer extends Reducer<Text, Text, Text, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> documentFrequency = new TreeMap<>();

        for (Text value : values) {
            String documentName = value.toString();
            documentFrequency.put(documentName, documentFrequency.getOrDefault(documentName, 0) + 1);
        }

        StringBuilder output = new StringBuilder();

        for (Map.Entry<String, Integer> entry : documentFrequency.entrySet()) {
            if (output.length() > 0) {
                output.append(", ");
            }

            output.append(entry.getKey())
                  .append(":")
                  .append(entry.getValue());
        }

        result.set(output.toString());
        context.write(key, result);
    }
}
