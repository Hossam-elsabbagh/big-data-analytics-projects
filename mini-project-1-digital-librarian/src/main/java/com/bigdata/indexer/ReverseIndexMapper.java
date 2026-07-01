package com.bigdata.indexer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReverseIndexMapper extends Mapper<Object, Text, Text, Text> {

    private final Text wordOut = new Text();
    private final Text documentOut = new Text();
    private final Set<String> stopwords = new HashSet<>();

    @Override
    protected void setup(Context context) {
        String[] defaults = {
            "the", "is", "at", "which", "on", "a", "an", "and", "or",
            "of", "to", "in", "for", "with", "as", "by", "from",
            "that", "this", "it", "be", "are", "was", "were", "has",
            "have", "had", "not", "but", "if", "then", "so", "than"
        };

        for (String word : defaults) {
            stopwords.add(word);
        }
    }

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        String cleanedLine = value.toString()
                .toLowerCase()
                .replaceAll("[^a-zA-Z0-9\\s]", " ");

        String[] tokens = cleanedLine.split("\\s+");

        for (String token : tokens) {
            if (token.length() > 1 && !stopwords.contains(token)) {
                wordOut.set(token);
                documentOut.set(fileName);
                context.write(wordOut, documentOut);
            }
        }
    }
}
