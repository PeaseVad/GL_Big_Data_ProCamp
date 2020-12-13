package com.globallogic.bdpc.mapreduce.airlines;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;

public class AirlinesMapperAgr extends Mapper<LongWritable, Text,  FloatWritable,Text> {
    private static HashMap<String, String> AirlinesMap = new HashMap<String, String>();
    private BufferedReader brReader;
    private String airlineName = "";


    enum MYCOUNTER {
     RECORD_COUNT, FILE_NOT_FOUND, SOME_OTHER_ERROR, PARSE_ERROR
  }


    @Override
    protected void setup(Context context) throws IOException {

        URI[] cacheFiles = context.getCacheFiles();
        String strLineRead;

        if (cacheFiles != null && cacheFiles.length == 1) {
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                while ((strLineRead = reader.readLine()) != null) {
                    String[] AirlinesArray = strLineRead.split(",");
                    AirlinesMap.put(AirlinesArray[0].trim(),
                            AirlinesArray[1].trim());
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
            } catch (IOException e) {
                context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
                e.printStackTrace();
            } finally {
                if (brReader != null) {
                    brReader.close();

                }

            }
        }


    }


    @Override
    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException {

        String line = lineText.toString();
        String lineArray[] = line.split("\\t");

        try {
            airlineName = AirlinesMap.get(lineArray[0]);
        } finally {
            airlineName = ((airlineName.equals(null) || airlineName
                    .equals("")) ? "NOT-FOUND" : airlineName);
        }

        float avg = 0.0f;
        try {
            avg = (-1) * Float.parseFloat(lineArray[1]);
        } catch (NumberFormatException nfe) {
            context.getCounter(MYCOUNTER.PARSE_ERROR).increment(1);
            return;
        }

        context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

        context.write(new FloatWritable(avg),new Text(airlineName));
    }
}
