package org.nh.mapreduce;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
//计算留存率：两日（6、7）、三日（5、6、7）、七日（1---7）
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.nh.mapreduce.newOldUser.newOldUserMapper;
import org.nh.mapreduce.newOldUser.newOldUserReducer;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class RetentionRate {

	public static class RetentionRateMapper extends Mapper<Text, Text, Text, Text>{
		private Text outputValue = new Text();

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
	
			String beginTime =  value.toString().split("\\s+")[2];
			String[] date = beginTime.split("T");
			String[] dates = date[0].split("-");
			outputValue.set(dates[2]);
			context.write(key, outputValue);

	   }
		
	}

	public static class RetentionRateReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		private Text outputkey = new Text();
		//两天都登陆的
		private int day2User = 0;
		//两天总人数
		private int day2Total = 0;
		//三天都登陆的
		private int day3User = 0;
		//三天总人数
		private int day3Total = 0;
		//七天都登陆的
		private int day7User = 0;
		//七天总人数
		private int day7Total = 0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context arg2)
				throws IOException, InterruptedException {
			boolean firstDay = false;
			boolean secondDay = false;
			boolean thirdDay = false;
			Set<String> set = new HashSet<String>();
			
			day7Total++;

			for (Text text : values) {
				String value = text.toString();
				set.add(value);
				
				if("07".equals(value)){
					firstDay = true;
				}else if ("06".equals(value)){
					secondDay =true;
				}else if("05".equals(value)){
					thirdDay = true;
				}
			}
			if(firstDay && secondDay){
				day2User++;
			}
			if(firstDay || secondDay){
				day2Total++;
			}
			if(firstDay && secondDay &&thirdDay){
				day3User++;
			}
			if (firstDay || secondDay || thirdDay) {
				day3Total++;
			}
			if(set.size() == 7){
				day7User++;
			}
					
			
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String date2Rate = String.format("%.2f", 1.0*day2User/day2Total *100)+"%";
			String date3Rate = String.format("%.2f", 1.0*day3User/day3Total *100)+"%";
			String date7Rate = String.format("%.2f", 1.0*day7User/day7Total *100)+"%";
			outputkey.set("6号7号两日留存率："+date2Rate + "\t" + "5号6号7号三日留存率：" + date3Rate + "\t" +"1号到7号七日留存率："+ date7Rate);
			context.write(outputkey, NullWritable.get());

		}


	}
	public static void main(String[] args) {
		
		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job;
			job = Job.getInstance(conf,"RetentionRate");

			job.setMapperClass(RetentionRateMapper.class);
			job.setReducerClass(RetentionRateReducer.class);

			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			Path inputPath1 = new Path("/ttj_GAMEPartition_Pro/part-r-00000");
			Path inputPath2 = new Path("/ttj_GAMEPartition_Pro/part-r-00001");
			FileInputFormat.addInputPath(job, inputPath1);
			FileInputFormat.addInputPath(job, inputPath2);			
			
			Path outputPath = new Path("/GAME_TotalDeviceList_ttj_heihiehei");
			FileSystem.get(conf).delete(outputPath, true);
			FileOutputFormat.setOutputPath(job, outputPath);

			System.exit(job.waitForCompletion(true)?0:1);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	}
}
