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
 *所有设备的系统中iOS和Android比率
 */
public class OScount {
	
	public static class OScountMapper extends Mapper<Text, Text, Text ,Text> {
		
		private Text outputValue =  new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			outputValue.set(value.toString().split("\\s+")[0]);
			context.write(key, outputValue);
		}
	}
	
	public static class OScountReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private Text outputkey = new Text();
		int iOS = 0;
		int Android = 0;
		int totoldevice = 0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
				totoldevice++;
				boolean flag = true;
				for (Text value : values) {
					if (flag && value.toString().equals("iOS")) {
						iOS++;
					}
					if (flag && value.toString().equals("Android")) {
						Android++;
					}
					flag = false;
				}
		}
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			String iosRate = String.format("%.2f", 1.0*iOS/totoldevice * 100) + "%";
			String androidRate = String.format("%.2f", 1.0*Android/totoldevice * 100) + "%";
			outputkey.set("登陆设备总数： "+totoldevice+"\t"+"安卓设备比率："+androidRate+"\t"+"iOS设备比率："+iosRate);
			context.write(outputkey, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "OScount");
		job.setJarByClass(OScount.class);
		
		job.setMapperClass(OScountMapper.class);
		job.setReducerClass(OScountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game-2017-01-01-2017-01-07.log"));
		
		Path outputPath = new Path("/nh-game-logs-3.9");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
