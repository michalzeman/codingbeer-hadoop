package com.codingbeer.hadoop.matrix.multiplication;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.StringReader;

import static com.codingbeer.hadoop.matrix.multiplication.MatrixMultiplicationCons.*;

/**
 * Created by zemo on 07/04/15.
 */
public class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * Matrix dimension are A 5x5 and for B 5X5
     * @param key
     * @param value JSON array in the form ["matrix_name", row, column, value]
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try (JsonReader reader = Json.createReader(new StringReader(value.toString()))) {
            JsonArray jsonArray = reader.readArray();
            //TODO: implementation hint input in form ["matrix",row,col,val]

        }
    }
}
