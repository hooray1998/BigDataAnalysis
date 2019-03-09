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
 *通过传参的形式对authority_的权限处理，求出各天的登陆人数
 */
public class BeforeOneday {

	public static class BeforeOnedayMapper extends Mapper<Text, Text, Text,Text> {
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
				
					context.write(key, value);
		}
	}
	public static class BeforeOnedayReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private int Sum1 = 0;
		private int Sum2 = 0;
		private int Sum3 = 0;
		private int Sum4 = 0;
		private int Sum5 = 0;
		private int Sum6 = 0;
		private int Sum7 = 0;
		private Text outputkey = new Text();
		//000208e3-0b15-48cc-bc87-aaf8d5bc48c4	121
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				int flag = Integer.parseInt(value.toString());
				if ((flag & 1) == 1) {
					Sum1++;
				}
				if ((flag & 2) == 2) {
					Sum2++;
				}
				if ((flag & 4) == 4) {
					Sum3++;
				}
				if ((flag & 8) == 8) {
					Sum4++;
				}
				if ((flag & 16) == 16) {
					Sum5++;
				}
				if ((flag & 32) == 32) {
					Sum6++;
				}
				if ((flag & 64) == 64) {
					Sum7++;
				}
			}
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			outputkey.set("第一天用户："+Sum1+"\t"+"前二天用户："+Sum2+"\t"+"前三天用户："+Sum3+"\t"+"前四天用户："+Sum4+"\t"+"前五天新用户："+Sum5+"\t"+"前六天新用户："+Sum6+"\t"+"前七天新用户："+Sum7+"\t");
			context.write(outputkey, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "BeforeOneday");
		job.setJarByClass(BeforeOneday.class);
		
		job.setMapperClass(BeforeOnedayMapper.class);
		job.setReducerClass(BeforeOnedayReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/nh-game-logs-3.6/3.6.1/part-r-00000"));
		
		Path outputPath = new Path("/nh-game-logs-4.1");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}

