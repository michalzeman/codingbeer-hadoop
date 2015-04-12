package com.codingbeer.hadoop.inverted.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by zemo on 02/04/15.
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try (JsonReader reader = Json.createReader(new StringReader(value.toString()))) {
            JsonArray jsonArray = reader.readArray();
            if (jsonArray.size() == 2) {
                //TODO: add implementation
                String words = jsonArray.getString(1);
                String document_id = jsonArray.getString(0);
                Text keyMap = new Text();
                Text valMap = new Text(document_id);
                Set<String> wordsSet = new HashSet<>();
                StringTokenizer stringTokenizer = new StringTokenizer(words, " \t\n\r\f,)(:;.?!/<>[]{}\"'");
                while(stringTokenizer.hasMoreElements()) {
                    String word = stringTokenizer.nextToken();
                    if (!wordsSet.contains(word)) {
                        wordsSet.add(word);
                        keyMap.set(word);
                        context.write(keyMap, valMap);
                    }
                }
            }
        }
    }
}
