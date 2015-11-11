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
            String matrix = jsonArray.getString(0);
            int row = jsonArray.getInt(1);
            int col = jsonArray.getInt(2);
            int val = jsonArray.getInt(3);

            StringBuilder valueOut = new StringBuilder(START_ARRAY_STR);
            valueOut.append("\"").append(matrix).append("\"").append(ARRAY_DELIMITER_STR).append(row)
                    .append(ARRAY_DELIMITER_STR).append(col).append(ARRAY_DELIMITER_STR)
                    .append(val).append(END_ARRAY_STR);
            for (int k = 0; k <= 4; k++) {
                StringBuilder keyOut = new StringBuilder();
                switch (matrix) {
                    case MATRIX_A:
                        keyOut.append(START_ARRAY_STR).append(row).append(ARRAY_DELIMITER_STR).append(k).append(END_ARRAY_STR);
                        break;
                    case MATRIX_B:
                        keyOut.append(START_ARRAY_STR).append(k).append(ARRAY_DELIMITER_STR).append(col).append(END_ARRAY_STR);
                        break;
                    default:
                        return;
                }
                context.write(new Text(keyOut.toString()), new Text(valueOut.toString()));
            }
        }
    }
}
