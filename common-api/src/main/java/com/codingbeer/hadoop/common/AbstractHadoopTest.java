package com.codingbeer.hadoop.common;

import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zemo on 21/03/15.
 */
public abstract class AbstractHadoopTest {

    protected TestFileInfo testFileInfo;

    protected List<Text> inputList = new ArrayList<>();

    protected List<Text> outList = new ArrayList<>();

    protected String getFileType() {
        return "json";
    }

    protected void setup() {
        testFileInfo = getTestFileInfo();
        inputList.addAll(readFileByLinesToList(testFileInfo.inFileName));
        outList.addAll(readFileByLinesToList(testFileInfo.outFileName));
    }

    protected TestFileInfo getTestFileInfo() {
        final String name = this.getClass().getSimpleName();
        return new TestFileInfo(name + "In." + getFileType(), name + "Out." + getFileType());
    }

    protected List<Text> readFileByLinesToList(final String fileName) {
        List<Text> result = new ArrayList<>();
        try (InputStream is = this.getClass().getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.add(new Text(line));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

}
