package com.codingbeer.hadoop.inverted.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by zemo on 02/04/15.
 */
public class InvertedIndexJob {

    static public void main(String[] arg) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted index");
        job.setJarByClass(InvertedIndexJob.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(arg[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
