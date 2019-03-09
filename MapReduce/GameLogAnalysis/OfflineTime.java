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
 *统计某天四个时间段的下线人数
 */
public class OfflineTime {
	
	public static class OfflineTimeMapper extends Mapper<Text, Text, Text, Text> {
		
		private Text outputvalue = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String date = value.toString().split("\\s+")[2];
			String Time = date.split("T")[1];
			outputvalue.set(Time);
			context.write(key, outputvalue);
		}
	}
	
	public static class OfflineTimeReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private Text outputkey = new Text();
		
		int totalplay = 0;
		int A1 = 0;
		int A2 = 0;
		int B1 = 0;
		int B2 = 0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
				for (Text value : values) {
					totalplay++;
					if ((value.toString().compareTo("00:00:00") > 0) && (value.toString().compareTo("06:00:00") <= 0)) {
						A1++;
					}else if ((value.toString().compareTo("06:00:00") > 0) && (value.toString().compareTo("12:00:00") <= 0)) {
						A2++;
					}else if ((value.toString().compareTo("12:00:00") > 0) && (value.toString().compareTo("18:00:00") <= 0)) {
						B1++;
					}else if ((value.toString().compareTo("18:00:00") > 0) && (value.toString().compareTo("24:00:00") <= 0)) {
						B2++;
					}
				}
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
				
			String A1Rate = String.format("%.2f", 1.0*A1/totalplay * 100) + "%";
			String A2Rate = String.format("%.2f", 1.0*A2/totalplay * 100) + "%";
			String B1Rate = String.format("%.2f", 1.0*B1/totalplay * 100) + "%";
			String B2Rate = String.format("%.2f", 1.0*B2/totalplay * 100) + "%";
				
				outputkey.set("下线总数：　"+totalplay+"\t"+"凌晨下线比率："+A1Rate+"\t"+"上午下线比率："+A2Rate+"\t"+"下午下线比率："+B1Rate+"\t"+"晚上下线比率："+B2Rate);
				context.write(outputkey, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "OfflineTime");
		job.setJarByClass(OfflineTime.class);
		
		job.setMapperClass(OfflineTimeMapper.class);
		job.setReducerClass(OfflineTimeReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		Path inputpath1 = new Path("/nh-game-logs-3.2/part-r-00000");
		Path inputpath2 = new Path("/nh-game-logs-3.2/part-r-00001");
		Path inputpath3 = new Path("/nh-game-logs-3.2/part-r-00002");
		Path inputpath4 = new Path("/nh-game-logs-3.2/part-r-00003");
		Path inputpath5 = new Path("/nh-game-logs-3.2/part-r-00004");
		Path inputpath6 = new Path("/nh-game-logs-3.2/part-r-00005");
		Path inputpath7 = new Path("/nh-game-logs-3.2/part-r-00006");
		int flag = 3;
		switch (flag) {
		case 1:
			FileInputFormat.addInputPath(job, inputpath1);
			break;
		case 2:
			FileInputFormat.addInputPath(job, inputpath2);
			break;
		case 3:
			FileInputFormat.addInputPath(job, inputpath3);
			break;
		case 4:
			FileInputFormat.addInputPath(job, inputpath4);
			break;
		case 5:
			FileInputFormat.addInputPath(job, inputpath5);
			break;
		case 6:
			FileInputFormat.addInputPath(job, inputpath6);
			break;
		case 7:
			FileInputFormat.addInputPath(job, inputpath7);
			break;
		default:
			break;
		}
		
		Path outputPath = new Path("/nh-game-logs-3.8.2");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
