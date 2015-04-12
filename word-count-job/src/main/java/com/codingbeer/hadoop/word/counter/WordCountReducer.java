package com.codingbeer.hadoop.word.counter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonWriter;
import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by zemo on 31/03/15.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //TODO: add implementation hint in form ["someWord",numberOfCount]
    }
}
