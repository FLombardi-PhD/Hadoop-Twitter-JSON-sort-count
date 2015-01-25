import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.text.SimpleDateFormat;
import java.text.ParseException;

public class TSCount {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
			boolean toEmit = true;
			String line = value.toString();
			if(line.startsWith(",")) line = line.replaceFirst(",", "");
			if(line.startsWith("[") || line.startsWith("]")){
				line = "{\"publishingdate\":[\"2000-01-01T00:00:00Z\"],\"title\":[\"start-end tweet\"]}";
				toEmit = false;
			}
			JSONObject json;
			Date date;
			String publishingdate = "";
			String timestamp = "0000000000";
			try {
				json = new JSONObject(line);
				publishingdate = json.getString("publishingdate");
				publishingdate = publishingdate.substring(2, 21).replace("T", " ");
				date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(publishingdate);
				timestamp = ""+date.getTime();
			} catch (JSONException e) { 
				System.out.println("ERROR while parsing the json: "+e.getMessage());
				System.err.println("INFO: An error occur (see error log): "+e.getMessage());
				timestamp = "JSON_ERROR";
			} catch (ParseException e) {
				System.out.println("ERROR while parsing the date: "+e.getMessage());
				System.err.println("INFO: An error occur (see error log): "+e.getMessage());
				timestamp = publishingdate;
			}
			word.set(timestamp);
			output.collect(word, one);
		}
	}

	public static class Reduce extends MapReduceBase implements	Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(TSCount.class);
		conf.setJobName("tscount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
