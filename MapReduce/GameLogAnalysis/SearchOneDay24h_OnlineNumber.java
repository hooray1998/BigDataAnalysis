package org.nh.mapreduce;

import java.io.IOException;
import javax.swing.JPanel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.log4j.BasicConfigurator;

public class SearchOneDay24h_OnlineNumber
{
	
	public static class SearchOneDay24h_OnlineNumberMapper extends Mapper<LongWritable, Text, IntWritable,IntWritable> 
    {
		
		private IntWritable outputKey = new IntWritable();
		private static IntWritable one = new IntWritable(1);

		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable,IntWritable>.Context context)//////////////////
				throws IOException, InterruptedException {
			
			String[] words = value.toString().split("\\s+");//---->   \\t 换行；    ''
			Integer startDay = Integer.parseInt(words[3].split("T")[0].substring(8, 10));
			Integer endDay   = Integer.parseInt(words[4].split("T")[0].substring(8, 10));
			
			Integer start = Integer.parseInt(words[3].split("T")[1].substring(0, 2));
			Integer end   = (endDay-startDay)*24 + Integer.parseInt(words[4].split("T")[1].substring(0, 2));

			for(int i=start+1;i<=end;i++)
			{
				outputKey.set(i%24);
				context.write(outputKey, one);
				if(i==24) break;
			}
		}
	}

	public static class SearchOneDay24h_OnlineNumberReducer extends Reducer<IntWritable, IntWritable, Text, LongWritable> 
    {
		
		private Text outputKey = new Text();
		private LongWritable result = new LongWritable();

		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,//在这之间自动聚合所有key相同的键值对，a, 1 , 1 
				Reducer<IntWritable, IntWritable, Text, LongWritable>.Context context) 
						throws IOException, InterruptedException 
        {
			
			long sum = 0;
			for (IntWritable value : values) 
            {
				sum++;
			}

			result.set(sum);
			
			outputKey.set(key.get()+"点在线人数:");
			context.write(outputKey, result);
		}
	}

	public static void main(String[] args) 
    {
		BasicConfigurator.configure();
		try 
        {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:9000");
            
            Job job;
            job=Job.getInstance(conf, "SearchOneDay24h_OnlineNumber");
            job.setJarByClass(SearchOneDay24h_OnlineNumber.class);	

            job.setMapperClass(SearchOneDay24h_OnlineNumberMapper.class);
            job.setReducerClass(SearchOneDay24h_OnlineNumberReducer.class);	

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            
			Path inputpath1 = new Path("/GAME_Partition/part-r-00000");
			Path inputpath2 = new Path("/GAME_Partition/part-r-00001");
			Path inputpath3 = new Path("/GAME_Partition/part-r-00002");
			Path inputpath4 = new Path("/GAME_Partition/part-r-00003");
			Path inputpath5 = new Path("/GAME_Partition/part-r-00004");
			Path inputpath6 = new Path("/GAME_Partition/part-r-00005");
			Path inputpath7 = new Path("/GAME_Partition/part-r-00006");
			int flag = 1;
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
			
			Path outputPath = new Path("/GAME/24hOnlineTime_1");
            FileSystem.get(conf).delete(outputPath,true);
            FileOutputFormat.setOutputPath(job, outputPath);
		
            System.exit(job.waitForCompletion(true)?0:1);

		} 
        catch (Exception e) ////////////////////////
        {
			e.printStackTrace();
		}
	}
}

