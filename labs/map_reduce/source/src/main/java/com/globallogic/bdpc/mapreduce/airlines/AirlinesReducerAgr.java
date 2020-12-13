package com.globallogic.bdpc.mapreduce.airlines;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



import java.io.IOException;

public class AirlinesReducerAgr extends Reducer<FloatWritable, Text, Text, FloatWritable> {

    static int count;

    @Override
    public void setup(Context context) {
        {
            count = 0;
        }
    }

    public void reduce(FloatWritable avgDelay, Iterable<Text> airlineNames, Context context)
            throws IOException, InterruptedException {

        float avg = (-1) * avgDelay.get();
        String airlineName = null;
        for (Text val : airlineNames) {
            airlineName = val.toString();
        }

        // we just write 5 records as output
        if (count < 5) {
            context.write(new Text(airlineName), new FloatWritable(avg));
            count++;
        }
    }
}