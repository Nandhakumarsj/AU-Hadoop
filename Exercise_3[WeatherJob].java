// WeatherJob.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class WeatherJob {
 public static void main(String[] args) throws Exception {
 if (args == null || args.length < 2) {
 System.err.println("Parameter Errors!
 Usages:<inputpath> <outputpath>");
 System.exit(-1);
 }
 Path inputPath = new Path(args[0]);
 Path outputPath = new Path(args[1]);
 Configuration conf = new Configuration();
 String jobName = WeatherJob.class.getSimpleName();
 Job job = Job.getInstance(conf, jobName);
 job.setJarByClass(WeatherJob.class);

 FileInputFormat.setInputPaths(job, inputPath);
 job.setInputFormatClass(TextInputFormat.class);
 job.setMapperClass(WeatherMapper.class);
 job.setMapOutputKeyClass(Text.class);
 job.setMapOutputValueClass(DoubleWritable.class);
 outputpath.getFileSystem(conf).delete(outputpath, true);
 FileOutputFormat.setOutputPath(job, outputPath);
 job.setOutputFormatClass(TextOutputFormat.class);
 job.setReducerClass(WeatherReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(DoubleWritable.class);
 job.setNumReduceTasks(1);
 job.waitForCompletion(true);
 }

 public static class WeatherMapper extends
 Mapper<LongWritable, Text, Text, DoubleWritable> {
 @Override
 protected void map(LongWritable k1, Text v1, Context context)
 throws IOException, InterruptedException {
 String line = v1.toString();
 Double max = null;
 Double min = null;
 try {
 max = Double.parseDouble(line.substring(103, 108));
 min = Double.parseDouble(line.substring(111, 116));
 } catch (NumberFormatException e) {
 return;
 }

 context.write(new Text("MAX"), new DoubleWritable(max));
 context.write(new Text("MIN"), new DoubleWritable(min));
 }
 }

 public static class WeatherReducer extends
 Reducer<Text, DoubleWritable, Text, DoubleWritable> {
 @Override
 protected void reduce(Text k2, Iterable<DoubleWritable>
 v2s, Context context)
 throws IOException, InterruptedException {
 double max = Double.MIN_VALUE;
 double min = Double.MAX_VALUE;
 if ("MAX".equals(k2.toString())) {
 for (DoubleWritable v2 : v2s) {
 double tmp = v2.get();
 if (tmp > max) {
 max = tmp;
 }
 }
 } else {
 for (DoubleWritable v2 : v2s) {
 double tmp = v2.get();
 if (tmp < min) {
 min = tmp;
 }
 }
 }
 context.write(k2, "MAX".equals(k2.toString()) ? new
 DoubleWritable(max) : new DoubleWritable(min));
 }
 }
}
