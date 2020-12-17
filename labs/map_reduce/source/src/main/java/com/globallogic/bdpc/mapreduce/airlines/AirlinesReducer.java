package com.globallogic.bdpc.mapreduce.airlines;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirlinesReducer extends Reducer<Text , IntWritable,  Text ,  FloatWritable > {

    // GLC| if you set the only reducer for the application
    // GLC| you can implement TopN algorithm right in here and write on Reducer.cleanup
    // GLC| so there is no need for extra MR applications

    public void reduce( Text airline,  Iterable<IntWritable> delays,  Context context)
            throws IOException,  InterruptedException {

        float sum  = 0.0f; // GLC| it's better to use double in java
        int count = 0; // GLC| long is aways better with MR
        for ( IntWritable delay  : delays) {
            sum  += delay.get();
            count++;
        }
        float avg =sum/count;

        // GLC| It's worth reusing writables.
        // GLC| You can initialise a writable along with the mapper instance and update its state before writing
        // GLC| private Text t = new Text();
        // ..
        // GLC| public void map >> t.set("some value"); context.write(t, ..);
        context.write(airline,  new FloatWritable(avg));
    }
}