package game.log;

import java.io.IOException;
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
import org.apache.log4j.BasicConfigurator;

/**
 * @author zjzy
 *统计七天准点在线人数
 */
public class playTime
{
	
	public static class playTimeMapper extends Mapper<LongWritable, Text, IntWritable,IntWritable> 
    {
		
		private IntWritable outputKey = new IntWritable();
		private static IntWritable one = new IntWritable(1);

		
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
			}
		}
	}

	public static class playTimeReducer extends Reducer<IntWritable, IntWritable, Text, LongWritable> 
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
            job=Job.getInstance(conf, "playTime");
            job.setJarByClass(playTime.class);	

            job.setMapperClass(playTimeMapper.class);
            job.setReducerClass(playTimeReducer.class);	

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            

            FileInputFormat.addInputPath(job, new Path("/game-2017-01-01-2017-01-07.log"));
            Path outputPath = new Path("/nh-game-logs-3.8.3");

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


