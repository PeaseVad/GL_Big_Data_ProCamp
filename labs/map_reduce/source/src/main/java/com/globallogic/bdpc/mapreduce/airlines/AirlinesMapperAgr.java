package com.globallogic.bdpc.mapreduce.airlines;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class AirlinesMapperAgr extends Mapper<LongWritable, Text,  FloatWritable,Text> {
    private static HashMap<String, String> AirlinesMap = new HashMap<String, String>(); // GLC| Java syntax for class members: camelCase
    private BufferedReader brReader; // GLC| some garbage ? :)
    private String airlineName = ""; // GLC| It should be defined in the map function


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
            // GLC| 1) there is a bug: if airlineName is null it will throw NPE
            // GLC| 2) airlineName.isEmpty() instead of airlineName.equals("")
            // GLC| 3) i'd use lineArray[0] or skip it with logging to a Counter instead of "NOT-FOUND"
            airlineName = ((airlineName.equals(null) || airlineName
                    .equals("")) ? "NOT-FOUND" : airlineName);
        }

        float avg = 0.0f;
        try {
            avg = (-1) * Float.parseFloat(lineArray[1]);
        } catch (NumberFormatException nfe) {
            // GLC| well done, it's the write way to use the Counters
            context.getCounter(MYCOUNTER.PARSE_ERROR).increment(1);
            return;
        }

        // GLC| No much sense to add such a counter as there is a MR common one
        context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

        // GLC| It's worth reusing writables.
        // GLC| You can initialise a writable along with the mapper instance and update its state before writing
        // GLC| private Text t = new Text();
        // GLC| ..
        // GLC| public void map >> t.set("some value"); context.write(t, ..);
        context.write(new FloatWritable(avg),new Text(airlineName));
    }
}
