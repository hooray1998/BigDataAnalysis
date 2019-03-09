package HeroAnalysis;

import java.io.IOException;
import java.text.DecimalFormat;

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

/*
 * 
 * 7.27
 * 玩家游戏总数、胜率、排名
 * 输入文件：heros.log
 * "Nunu,0"	#	57
 * 
 *  阶段1：整理数据
 *
 */
public class AllPlayerInfo {
	public static class AllPlayerInfoMapper extends Mapper<Text, Text, Text, Text>{
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String word[] = value.toString().split("\\s+");
			outputKey.set(word[1]);
			outputValue.set(word[0]);
			context.write(outputKey, outputValue);
		}
	}
	public static class AllPlayerInfoReducer extends Reducer<Text, Text, Text, Text>{
	    
		private Text outputValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int win_num = 0;
			for (Text value : values) {
				sum++;
				if(value.toString().equals("1")){
					win_num++;
				}
			}
			double rate = 1.0*win_num/sum*100;
			DecimalFormat df = new DecimalFormat(".00");
//			outputValue.set(sum + "\t" + win_num + "\t" + df.format(rate) +"%");
			outputValue.set(sum + "\t\t" + win_num + "\t\t" + df.format(rate) + " %");
			context.write(key, outputValue);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job=Job.getInstance(conf, "AllPlayerInfo");
		job.setJarByClass(AllPlayerInfo.class);
		
		//设置专属的map和reduce
		job.setMapperClass(AllPlayerInfoMapper.class);
		job.setReducerClass(AllPlayerInfoReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//设置文件的输入和输出位置
		FileInputFormat.addInputPath(job, new Path("/HERO/HERO_NEW"));
		Path outputPath = new Path("/HERO/PlayerInfo");
		//如果输出路径已存在，先删除
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
