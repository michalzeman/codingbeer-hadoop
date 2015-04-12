package com.codingbeer.hadoop.matrix.multiplication;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.json.*;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import static com.codingbeer.hadoop.matrix.multiplication.MatrixMultiplicationCons.*;

/**
 * Created by zemo on 07/04/15.
 */
public class MatrixMultiplicationReducer extends Reducer<Text, Text, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //TODO: add implementation, hint output in form [row,col,value]
    }
}
