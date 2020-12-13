package com.globallogic.bdpc.mapreduce.airlines;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirlinesReducer extends Reducer<Text , IntWritable,  Text ,  FloatWritable > {

    public void reduce( Text airline,  Iterable<IntWritable> delays,  Context context)
            throws IOException,  InterruptedException {

        float sum  = 0.0f;
        int count = 0;
        for ( IntWritable delay  : delays) {
            sum  += delay.get();
            count++;
        }
        float avg =sum/count;

        context.write(airline,  new FloatWritable(avg));
    }
}