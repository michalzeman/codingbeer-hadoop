package com.codingbeer.hadoop.word.counter;


import com.codingbeer.hadoop.common.AbstractHadoopTest;
import com.codingbeer.hadoop.word.counter.WordCountMapper;
import com.codingbeer.hadoop.word.counter.WordCountReducer;
import org.apache.hadoop.io.IntWritable;
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
 * Created by zemo on 14/03/15.
 */
public class WordCountJobTest extends AbstractHadoopTest {

    MapReduceDriver<LongWritable, Text, Text, IntWritable, NullWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {
        setup();
        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();
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
