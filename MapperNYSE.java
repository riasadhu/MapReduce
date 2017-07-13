package NYSESLT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapperNYSE {

	public static class InputMapClass extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, Context context) {
			try {
				context.write(key, value);

			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("name", "value")
		conf.setInt("mapreduce.input.fileinputformat.split.maxsize", 128*1024*1024);
		
		
		Job job = Job.getInstance(conf, "Volume Count");
		job.setJarByClass(MapperNYSE.class);
		job.setMapperClass(InputMapClass.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 FileSystem.get(conf).delete(new Path(args[1]),true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}