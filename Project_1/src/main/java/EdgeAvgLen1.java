import java.io.IOException;
import java.util.*;

import jdk.nashorn.internal.runtime.arrays.ArrayLikeIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeAvgLen1 {

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

        private static Map<String, ArrayList<Double>> map = new HashMap<String, ArrayList<Double>>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()," ");
            List<String> list = new ArrayList<String >();
            while (itr.hasMoreTokens()) {
                list.add(itr.nextToken().toString());
            }
            //we use the such form "NodeId,length of in-coming edges"(eg."1,2") as the map key, the value is always 1 here
            String mapKey = list.get(2) + "," + list.get(3);
            //If we found the key has been in the map, then we append the new value into its value which is a list.
            if(map.containsKey(mapKey)){
                ArrayList<Double> arrayList = map.get(mapKey);
                arrayList.add(new Double(1));
                map.put(mapKey, arrayList);
            } else { // If not, we create a new set into the map.
                ArrayList<Double> L = new ArrayList<Double>();
                L.add(new Double(1));
                map.put(mapKey, L);
            }
        }

        // In the cleanup function, we split the key. Then, the form of output value becomes like "length of in-coming edges, size".
        // The size is the total number of one spec in-coming length of the edge, eg."3,2",
        // which means there are two edges of length 3 in-coming.
        public void cleanup(Context context) throws IOException, InterruptedException {
            Set<Map.Entry<String, ArrayList<Double>>> sets = map.entrySet();
            for(Map.Entry<String, ArrayList<Double>> set : sets){
                double num = set.getValue().size();
                String[] strings = set.getKey().split(",");
                String newValue = strings[1]+","+num;
                context.write(new IntWritable(Integer.parseInt(strings[0].toString())), new Text(newValue));
            }
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {
        private DoubleWritable doubleWritable = new DoubleWritable();

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0, num = 0;
            for (Text t : values){
                //Split the value into a string list, the first element is the in-coming length,
                // another one is total number of the in-coming length.
                String[] strings = t.toString().split(",");
                double number = Double.parseDouble(strings[1].toString());
                double inComingValue = Double.parseDouble(strings[0].toString());
                sum += number * inComingValue;
                num += number;
            }
            double result = sum * 1.0 / num * 1.0;
            doubleWritable.set(result);
            context.write(key, doubleWritable);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "in-coming average");
        job.setJarByClass(EdgeAvgLen1.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
