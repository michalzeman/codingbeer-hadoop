package com.codingbeer.hadoop.matrix.multiplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by zemo on 07/04/15.
 */
public class MatrixMultiplicationJob {

    static public void main(String[] arg) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted index");
        job.setJarByClass(MatrixMultiplicationJob.class);
        job.setMapperClass(MatrixMultiplicationMapper.class);
        job.setReducerClass(MatrixMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(arg[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
