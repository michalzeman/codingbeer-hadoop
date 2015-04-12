package com.codingbeer.hadoop.word.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by zemo on 14/03/15.
 */
public class WordCountJob {

    static public void main(String[] arg) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word count");
        job.setJarByClass(WordCountJob.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(arg[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
