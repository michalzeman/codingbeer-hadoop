package com.codingbeer.hadoop.inverted.index;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;

/**
 * Created by zemo on 02/04/15.
 */
public class InvertedIndexReducer extends Reducer<Text, Text, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        try (StringWriter stringWriter = new StringWriter();
             JsonWriter jsonWriter = Json.createWriter(stringWriter)) {
            JsonArrayBuilder jsonArray = Json.createArrayBuilder();
            JsonArrayBuilder resultArray = Json.createArrayBuilder();

            //TODO: add implementation hint output should be in format ["word",["resource_1.txt","resource_2.txt"]]
            resultArray.add(key.toString());
            values.iterator().forEachRemaining(documentId -> jsonArray.add(documentId.toString()));
            resultArray.add(jsonArray);
            jsonWriter.writeArray(resultArray.build());
            context.write(NullWritable.get(), new Text(stringWriter.getBuffer().toString()));
        }
    }
}
