import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class EdgeAvgLen2 {

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable word = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()," ");
            List<String> list = new ArrayList<String >();
            while (itr.hasMoreTokens()) {
                list.add(itr.nextToken().toString());
            }
            String edge = list.get(2);
            String inComingValue = list.get(3);
            word.set(Integer.parseInt(edge));
            //the output value is formed as "the length of in-coming edge, 1", eg."3,1",which means the length is 3, and number is 1
            context.write(word, new Text(inComingValue + ",1"));
        }
    }


    public static class EdgeAvgLen2Combiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        // Reduce Method
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0, num = 0;
            //Split the value of input into a string list, the first element is the length of in-coming edge
            // another element is 1.
            for(Text t : values){
                String[] strings = t.toString().split(",");
                sum += Double.parseDouble(strings[0].toString());
                num += Double.parseDouble(strings[1].toString());
            }
            // the form of output value is like "sum, num", the sum is the total sum of the same length of in-coming edge.
            // num is the number of the same length of in-coming edge.
            String v = new String(sum + ","+num);
            System.out.println(v);
            context.write(key, new Text(v));
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int num = 0;
            // Add up all in-coming edge of the node
            for (Text t : values){
                String[] strings = t.toString().split(",");
                int temp = Integer.parseInt(strings[1].toString());
                sum += Double.parseDouble(strings[0].toString());
                num += temp;
            }
            context.write(new IntWritable(Integer.parseInt(key.toString())), new DoubleWritable(sum * 1.0 / num));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "in-coming average");
        job.setJarByClass(EdgeAvgLen2.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(EdgeAvgLen2Combiner.class);
        job.setReducerClass(IntSumReducer.class);


        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
