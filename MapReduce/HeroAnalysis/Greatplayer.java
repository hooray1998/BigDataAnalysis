package HeroAnalysis2;

import java.io.IOException;
import java.util.function.IntPredicate;

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

/**
 * @author zjzy
 *传参进入英雄分区文件 统计所有玩家使用该英雄的胜率
 */
public class Greatplayer {
	
	public static class GreatplayerMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	//2-Fiora     	0	1	Help	Assassin	17	11	61	906
	public static class GreatplayerReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text outputkey = new Text();
		private Text outputvalue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			int[] wincount = new int[101];
			int[] allcount = new int[101];
			float[] winrate = new float[101];
			
			for (Text value : values) {
				String[] words = value.toString().split("\\s+");
				if(Integer.parseInt(words[0])==1){
					wincount[Integer.parseInt(words[1])]++;
					allcount[Integer.parseInt(words[1])]++;
				}else {
					allcount[Integer.parseInt(words[1])]++;
				}
			}
			int max_num = 1;
			float max = 0;
			System.out.println(max);
			for(int i=0;i<101;i++){
				if(allcount[i]!=0){
					winrate[i] = (float) (1.0*wincount[i]/allcount[i]);
				} else {
					winrate[i] = 0;
				}
				System.out.println(wincount[i]+" "+allcount[i]+" "+winrate[i]);
				if(max<winrate[i]){
					max = winrate[i];
					max_num = i;
					System.out.println(i);
				}
			}
			outputkey = key;
			String out = String.format("%.2f",winrate[max_num]);
			outputvalue.set(","+max_num+","+out);
			System.out.println(outputvalue.toString());
			context.write(outputkey, outputvalue);
		}
	}
	public static void main(String[] args) throws Exception {
		
		//args[]代表的是参数列表，默认以逗号或空格分割
//		id = Integer.parseInt(args[0]);
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "Greatplayer");
		job.setJarByClass(Greatplayer.class);
		
		job.setMapperClass(GreatplayerMapper.class);
		job.setReducerClass(GreatplayerReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
//		if(id>9){
//			FileInputFormat.addInputPath(job, new Path("/nh-hero-3.0/part-r-000"+id));
//		} else {
//			FileInputFormat.addInputPath(job, new Path("/nh-hero-3.0/part-r-0000"+id));
//		}
		FileInputFormat.addInputPath(job, new Path("/nh-hero-3.0"));
		Path outputPath = new Path("/nh-hero-3.0_");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
