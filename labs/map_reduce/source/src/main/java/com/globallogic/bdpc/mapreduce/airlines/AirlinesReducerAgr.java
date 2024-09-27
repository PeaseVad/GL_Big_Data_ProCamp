package com.globallogic.bdpc.mapreduce.airlines;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirlinesReducerAgr extends Reducer<FloatWritable, Text, Text, FloatWritable> {

    static int count; // GLC: private long count

    @Override
    public void setup(Context context) {
        {
            count = 0;
        }
    }

    public void reduce(FloatWritable avgDelay, Iterable<Text> airlineNames, Context context)
            throws IOException, InterruptedException {

        // GLC| There is a bug:
        // GLC| if there are more than one airline with the same avgDelay the code picks up only the last one
        float avg = (-1) * avgDelay.get();
        String airlineName = null;
        for (Text val : airlineNames) {
            airlineName = val.toString();
        }

        // we just write 5 records as output
        if (count < 5) {
            // GLC| It's worth reusing writables.
            // GLC| You can initialise a writable along with the mapper instance and update its state before writing
            // GLC| private Text t = new Text();
            // ..
            // GLC| public void map >> t.set("some value"); context.write(t, ..);
            context.write(new Text(airlineName), new FloatWritable(avg));
            count++;
        }
    }
}