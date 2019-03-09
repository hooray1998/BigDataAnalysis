package eCommerceAnalyse;

import java.io.IOException;
import java.text.DecimalFormat;

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
 * 统计各用户有效消费金额（付款的前提下），购买次数
 * 每个用户id，累计消费金额，累计消费次数
 * @author leowxm
 *
 */

public class UserInfo {
	
	public static class UserInfoMapper extends Mapper<Text, Text, Text, Text> {
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().split("\\s+");
			if (Integer.parseInt(words[0]) == 1) {
				context.write(key, value);
			}
		}
		
	}
	

	public static class UserInfoReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private Text outputKey = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			//累计消费金额
			double Total_money = 0.00;
			//累计消费次数
			int Total_number = 0;
			
			for (Text value : values) {
				
				String[] words = value.toString().split("\\s+");
				
				Total_number++;
				Total_money += Double.parseDouble(words[6]);
				
			}
			//强制保留小数点后两位
			DecimalFormat df = new DecimalFormat("#####0.00"); 
			String str = df.format(Total_money);
			
			outputKey.set(key + "\t\t累计消费金额："+str+"\t\t累计消费次数："+Total_number);
			context.write(outputKey, NullWritable.get());
			
		}
		
	}
	
	public static void main(String[] args) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");//设置端口

			Job job;
			job=Job.getInstance(conf, "UserInfo");
			job.setJarByClass(UserInfo.class);

			job.setMapperClass(UserInfoMapper.class);
			job.setReducerClass(UserInfoReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);	
			

			FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
			Path outputPath = new Path("/Test/UserInfo");

			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);

			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}						
		
	}

}
