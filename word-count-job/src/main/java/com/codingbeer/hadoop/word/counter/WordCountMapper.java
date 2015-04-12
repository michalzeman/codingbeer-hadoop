package com.codingbeer.hadoop.word.counter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zemo on 31/03/15.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);

    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                //TODO: add implementation out in form /word/,1
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), " \t\n\r\f,)(:;.?!/<>[]");
        while (stringTokenizer.hasMoreTokens()) {
            word.set(stringTokenizer.nextToken());
            context.write(word, one);
        }
    }
}