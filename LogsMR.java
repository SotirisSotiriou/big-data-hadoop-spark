import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogsMR {

  public static class LogMapper extends Mapper<Object, Text, Text, Text>{
    private Text outputkey = new Text();
    private Text outputvalue = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	//break the input files into lines
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), System.lineSeparator());
        while(tokenizer.hasMoreTokens()){
            String[] attr = tokenizer.nextToken().split(",");
            String ip = attr[0];
            String date = attr[1];
            String accession = attr[5];
            String extension = attr[6];
            String filename = extension;
            if(extension.charAt(0) == '.'){
            	filename = accession + extension;
            }
            
            outputkey.set(ip.concat(" ").concat(filename));
            outputvalue.set(date);
            
            //<key, value> --> <ip, filename>
            context.write(outputkey, outputvalue);
        }
    }
  }

  public static class LogReducer extends Reducer<Text,Text,Text,Text> {
	  	private Text outputvalue = new Text();
	  	private Text outputkey = new Text();
	  	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			HashSet<Text> dates = new HashSet<Text>();
			
			//get unique dates
			for(Text val : values) {
				dates.add(val);
			}
			
			if(dates.size() > 1) {
				String[] attributes = key.toString().split(" ");
				String ip = attributes[0];
				String filename = attributes[1];
				outputkey.set(ip);
				outputvalue.set(filename);
				context.write(outputkey, outputvalue);
			}
		}
  }

  public static void main(String[] args) throws Exception {
	  	int numOfReducerTasks;
	  	try {
	  		numOfReducerTasks = Integer.parseInt(args[2]);
	  		System.out.println("Reducer Tasks: " + Integer.toString(numOfReducerTasks));
	  	} catch(NumberFormatException e) {
	  		System.exit(1);
	  	}
	  	numOfReducerTasks = Integer.parseInt(args[2]);
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "log mapred " + "input: " + args[0] + ", output: " + args[1] + ", Reduce Tasks: " + args[2]);
	    job.setJarByClass(LogsMR.class);
	    job.setMapperClass(LogMapper.class);
	    job.setReducerClass(LogReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(numOfReducerTasks);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}


