package com.globallogic.bdpc.mapreduce.airlines;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class TopAirlines {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TopAirlines <input path> <output path>");
            System.exit(-1);
        }

        // create a Hadoop job and set the main class
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"Job1"  );
        job.setJarByClass(TopAirlines.class);
        job.setJobName("TopAirlines");

        // set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the Mapper and Reducer class
        job.setMapperClass(AirlinesMapper.class);
        job.setReducerClass(AirlinesReducer.class);

        // specify the type of the output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }


        Job jobAgr = Job.getInstance(conf,"Job2");
        jobAgr.setJarByClass(TopAirlines.class);
        jobAgr.setJobName("TopAirlinesAgr");
        jobAgr.addCacheFile(new Path("/bdpc/hadoop_mr/dict/airlines.csv").toUri());


        // set the input and output path
        FileInputFormat.addInputPath(jobAgr, new Path(args[1]));
        FileOutputFormat.setOutputPath(jobAgr, new Path(args[2]));

        // set the Mapper and Reducer class
        jobAgr.setMapperClass(AirlinesMapperAgr.class);
        jobAgr.setReducerClass(AirlinesReducerAgr.class);

        // specify the type of the output
        jobAgr.setOutputKeyClass(FloatWritable.class);
        jobAgr.setOutputValueClass(Text.class);
        jobAgr.setNumReduceTasks(1);

        // run the job
        if (!jobAgr.waitForCompletion(true)) {
            System.exit(1);
        }

        Counters cn =jobAgr.getCounters();
        Counter c1=cn.findCounter(AirlinesMapperAgr.MYCOUNTER.RECORD_COUNT);
        System.out.println(c1.getDisplayName()+":"+c1.getValue());
        Counter c2=cn.findCounter(AirlinesMapperAgr.MYCOUNTER.PARSE_ERROR);
        System.out.println(c2.getDisplayName()+":"+c2.getValue());
    }
}

