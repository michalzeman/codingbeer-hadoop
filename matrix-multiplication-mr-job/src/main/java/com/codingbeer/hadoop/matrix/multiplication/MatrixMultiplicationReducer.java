package com.codingbeer.hadoop.matrix.multiplication;

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
        int[] a = new int[5];
        int[] b = new int[5];

        int[] keyArray = new int[2];
        try (JsonReader reader = Json.createReader(new StringReader(key.toString()))) {
            JsonArray jsonArrayKey = reader.readArray();
            for (int i = 0; i<jsonArrayKey.size(); i++) {
                keyArray[i] = jsonArrayKey.getInt(i);
            }
        }

        values.iterator().forEachRemaining(value -> {
            try (JsonReader reader = Json.createReader(new StringReader(value.toString()))) {
                JsonArray jsonArrayValue = reader.readArray();
                String matrix = jsonArrayValue.getString(0);
                switch (matrix) {
                    case MATRIX_A:
                        int col = jsonArrayValue.getInt(2);
                        a[col] = jsonArrayValue.getInt(3);
                        break;
                    case MATRIX_B:
                        int row = jsonArrayValue.getInt(1);
                        b[row] = jsonArrayValue.getInt(3);
                        break;
                    default:
                        break;
                }
            }
        });
        int sum = 0;
        for (int i = 0; i < 5; i++) {
            sum +=(a[i]*b[i]);
        }
        try (StringWriter stringWriter = new StringWriter();
             JsonWriter jsonWriter = Json.createWriter(stringWriter)) {
            JsonArrayBuilder jsonArray = Json.createArrayBuilder();
            jsonArray.add(keyArray[0]);
            jsonArray.add(keyArray[1]);
            jsonArray.add(sum);
            jsonWriter.writeArray(jsonArray.build());
            context.write(NullWritable.get(), new Text(stringWriter.getBuffer().toString()));
        }
    }
}
