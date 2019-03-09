package eCommerceAnalyse;

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


/*
 * NoThinkCount.java\ThinkCount.java\ThinkTime.java的合并
 * 统计多少人购买时无考虑时间(下单时间等于上线时间)，并计算男女在其中分别占有的比重。
 * 统计多少人购买时有考虑时间(下单时间等于上线时间)，并计算男女在其中分别占有的比重。
 * 统计多少人购买时有考虑时间(下单时间等于上线时间)，并计算男女在其中分别占有的比重。
 * 
 * 用户唯一标识
 * 在线时间
 * 上线次数
 * 首次登录时间
 * 
 */
public class ThinkTime {
	public static class ThinkTimeMapper extends Mapper<Text, Text, Text, Text>{

		private Text outputValue = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			int dif = 0;
			int startdd=0;
			int enddd=0;
			Integer sex = Integer.parseInt(value.toString().split("\\s+")[1]);
			String startString = value.toString().split("\\s+")[2];
			String endString = value.toString().split("\\s+")[3];
			Integer startYear= Integer.parseInt(startString.split("-")[0]);
			Integer endYear  = Integer.parseInt(endString.split("-")[0]);
			Integer startMonth= Integer.parseInt(startString.split("-")[1]);
			Integer endMonth  = Integer.parseInt(endString.split("-")[1]);
			Integer startDay= Integer.parseInt(startString.split("-")[2]);
			Integer endDay  = Integer.parseInt(endString.split("-")[2]);
			
			dif += (endYear-startYear)*365;
			switch (startMonth) {

				case 12:	startdd += 30;
				case 11:	startdd += 31;
				case 10:		startdd += 30;
				case 9:		startdd += 31;
				case 8:		startdd += 31;
				case 7:		startdd += 30;
				case 6:		startdd += 31;
				case 5:		startdd += 30;
				case 4:		startdd += 31;
				case 3:		startdd += 29;
				case 2:		startdd += 31;
			}
			switch (endMonth) {

				case 12:	enddd += 30;
				case 11:	enddd += 31;
				case 10:		enddd += 30;
				case 9:		enddd += 31;
				case 8:		enddd += 31;
				case 7:		enddd += 30;
				case 6:		enddd += 31;
				case 5:		enddd += 30;
				case 4:		enddd += 31;
				case 3:		enddd += 29;
				case 2:		enddd += 31;
			}
			dif = dif + enddd -startdd + endDay - startDay;
			
			outputValue.set(sex+"="+dif);
			
			context.write(key, outputValue);
		}
	}
	public static class ThinkTimeReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private Text outputKey = new Text();
		private int allCount = 0;
		private int noThinkCount = 0;
		private int noThinkCount_man = 0;
		private int noThinkCount_woman = 0;
		private int ThinkCount = 0;
		private long manCount= 0;
		private long womanCount= 0;
		private long manTime= 0;
		private long womanTime= 0;
		private long allTime= 0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			for (Text value : values) {
				String words[] = value.toString().split("=");
				int sex = Integer.parseInt(words[0]);
				int time = Integer.parseInt(words[1]);
				
				allCount++;
				
				allTime+=time;
				if(sex==2){
					if(time==0)	{
						noThinkCount++;
						noThinkCount_woman++;
					}else{
						ThinkCount++;
						womanCount++;
						womanTime+=time;
					}
				}
				else{
					if(time==0)	{
						noThinkCount++;
						noThinkCount_man++;
					}else{
						ThinkCount++;
						manCount++;
						manTime+=time;
					}
				}
				//context.write(key, value);
			}
		}
		
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			outputKey.set("总次数：\t"+allCount+
							"\n不犹豫数：\t"+noThinkCount+"\t比重：	"+100*(float)noThinkCount/allCount+
							"\n男生不犹豫数：\t"+noThinkCount_man+"\t比重：	"+100*(float)noThinkCount_man/allCount+
							"\n女生不犹豫数：\t"+noThinkCount_woman+"\t比重：	"+100*(float)noThinkCount_woman/allCount+
							"\n犹豫数：\t"+ThinkCount+"\t比重：	"+100*(float)ThinkCount/allCount+
							"\n男生犹豫数：\t"+manCount+"\t比重：	"+100*(float)manCount/allCount+
							"\n女生犹豫数：\t"+womanCount+"\t比重：	"+100*(float)womanCount/allCount+
							"\n总犹豫时间：\t"+allTime+
							"\n男生犹豫时间：\t"+manTime+"\t比重：	"+100*(float)manTime/allTime+
							"\n女生犹豫时间：\t"+womanTime+"\t比重：	"+100*(float)womanTime/allTime);

			context.write(outputKey,NullWritable.get());
		
			
			
		}
	}
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");//设置端口

			Job job;
			job=Job.getInstance(conf, "ThinkTime");
			job.setJarByClass(ThinkTime.class);

			job.setMapperClass(ThinkTimeMapper.class);
			job.setReducerClass(ThinkTimeReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);	

			//多文件输入
			Path inputPath1 = new Path("/SHOP_LOG");
			FileInputFormat.addInputPath(job,inputPath1);			
			
			Path outputPath = new Path("/Test/ThinkTime_F&M");
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);

			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
}

