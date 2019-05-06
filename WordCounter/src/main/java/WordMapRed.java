import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordMapRed {
	
	static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line[] = value.toString().split(" ");
			
			for(String word : line){ 
				System.out.println(word + " 1");
				context.write(new Text(word.toUpperCase()), new IntWritable(1));
			}
			
		}
		
	}
	
	static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for(IntWritable value : values) sum += value.get();
			
			context.write(key, new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws Exception{
				
		if(args.length != 2){
			System.err.println("Wrong arguments");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(WordMapRed.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
