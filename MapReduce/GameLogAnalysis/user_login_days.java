package org.nh.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//取出前20名（排序规则：累计时长、上线次数、首登时间）

/**
 * user_login_days.java
 * @author itt
 * 2018年7月25日 上午11:42:21
 * 
 * 
 */
public class user_login_days {
	public static class user_login_daysMapper extends Mapper<Text, Text, Text, Text> {
		
		private Text outputValue =new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			
			String day = value.toString().split("\\s+")[2].split("T")[0].split("-")[2];
			outputValue.set(day);
			context.write(key, outputValue);
			//System.out.println(key.toString()+"\t"+day);
			
		}
	}
	
	public static class user_login_daysReducer extends Reducer<Text,Text,Text, Text>{
		

		private Text outputValue = new Text();
		
			
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			
			boolean firstDay = false;
			boolean secondDay = false;
			boolean thirdDay= false;
			boolean forthDay= false;
			boolean fifthDay= false;
			boolean sixthDay= false;
			boolean seventhDay= false;

			for (Text value : values) {
				String valueString = value.toString();
				if ("01".equals(valueString)) {
					firstDay = true;	
				}else if("02".equals(valueString)){
					secondDay = true;
				}else if("03".equals(valueString)){
					thirdDay = true;
				}else if("04".equals(valueString)){
					forthDay = true;
				}else if("05".equals(valueString)){
					fifthDay = true;
				}else if("06".equals(valueString)){
					sixthDay = true;
				}else{
					seventhDay = true;
			}
			
		}	
			
			String out = new String(" ");
			if(firstDay)	out = out.concat(" 1");
			if(secondDay)	out = out.concat(" 2");
			if(thirdDay)	out = out.concat(" 3");
			if(forthDay)	out = out.concat(" 4");
			if(fifthDay)	out = out.concat(" 5");
			if(sixthDay)	out = out.concat(" 6");
			if(seventhDay)	out = out.concat(" 7");

			outputValue.set(out);
			
			context.write(key, outputValue);
			
	}
	}

	public static void main(String[] args) {
		
		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job;
			job = Job.getInstance(conf,"user_login_days");

			job.setMapperClass(user_login_daysMapper.class);
			job.setReducerClass(user_login_daysReducer.class);
						
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.addInputPath(job, new Path("/GAME_LOG"));
			Path outputPath = new Path("/GAME-LOGIN-DAYS");
			
			FileSystem.get(conf).delete(outputPath, true);
			FileOutputFormat.setOutputPath(job, outputPath);

			System.exit(job.waitForCompletion(true)?0:1);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

