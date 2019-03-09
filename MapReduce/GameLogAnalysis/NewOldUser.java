package game.log;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author zjzy
 *课上老师带写。第二天新老用户
 */
public class NewOldUser {
	
	public static class NewOldUserMapper extends Mapper<Text, Text, Text, Text> {
		
		private Text outputvalue = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String time = value.toString().split("\\s+")[2];
			String date = time.split("T")[0];
			String[] day= date.split("-");
			outputvalue.set(day[2]);
			context.write(key, outputvalue);
		}
	}
	
	public static class NewOldUserReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		private Text outputkey = new Text();
		
		int oldusers = 0;
		int newusers = 0;
		
		int total_device_new = 0;
		int total_device_old = 0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			boolean first = false;
			boolean second = false;
			for (Text value : values) {
				if("01".equals(value.toString())){
					first = true;
				}else if ("02".equals(value.toString())) {
					second = true;
				}
			}
			if (first && second) {
				oldusers++;
			}else if (!first && second) {
				newusers++;
			}
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			outputkey.set("newusers: "+newusers+"\t"+"oldusers: "+oldusers);
			context.write(outputkey, NullWritable.get());
		}
	}
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "NewOldUser");
		job.setJarByClass(NewOldUser.class);
		
		job.setMapperClass(NewOldUserMapper.class);
		job.setReducerClass(NewOldUserReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//FileInputFormat.addInputPath(job, new Path("/game-2017-01-01-2017-01-07.log"));
		
		Path inputpath1 = new Path("/nh-game-logs-3.2/part-r-00000");
		Path inputpath2 = new Path("/nh-game-logs-3.2/part-r-00001");
		FileInputFormat.addInputPath(job, inputpath1);
		FileInputFormat.addInputPath(job, inputpath2);
		
		Path outputPath = new Path("/nh-game-logs-3.3");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
