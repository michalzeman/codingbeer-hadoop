package com.codingbeer.hadoop.matrix.multiplication;

import com.codingbeer.hadoop.common.AbstractHadoopTest;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by zemo on 07/04/15.
 */
public class MatrixMultiplicationJobTest extends AbstractHadoopTest {

    MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {
        setup();
        MatrixMultiplicationMapper mapper = new MatrixMultiplicationMapper();
        MatrixMultiplicationReducer reducer = new MatrixMultiplicationReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() throws IOException {
        for (Text in : inputList) {
            mapReduceDriver.withInput(new LongWritable(), in);
        }
        List<Pair<NullWritable, Text>> outputRecords =
                outList.stream().map(outLine -> new Pair<>(NullWritable.get(), outLine)).collect(Collectors.toList());
        mapReduceDriver.withAllOutput(outputRecords);
        mapReduceDriver.runTest(false);
    }
}
