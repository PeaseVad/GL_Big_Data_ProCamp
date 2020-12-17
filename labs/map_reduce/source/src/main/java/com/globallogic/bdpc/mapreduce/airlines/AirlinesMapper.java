package com.globallogic.bdpc.mapreduce.airlines;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirlinesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException {
        String line = lineText.toString();
        String airline = line.split(",")[4];
        int delay = 0;
        try {
            // GLC| it's better to avoid creation of the split array for the second time
            delay = Integer.parseInt(line.split(",")[11]);
        } catch (NumberFormatException nfe) {
            // GLC| good practice is to count exceptions with Counters
            return;
        }

        // GLC| It's worth reusing writables.
        // GLC| You can initialise a writable along with the mapper instance and update its state before writing
        // GLC| private Text t = new Text();
        // GLC| ..
        // GLC| public void map >> t.set("some value"); context.write(t, ..);
        context.write(new Text(airline), new IntWritable(delay));
    }
}
