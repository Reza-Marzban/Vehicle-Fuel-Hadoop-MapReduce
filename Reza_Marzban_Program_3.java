import java.util.HashSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class Program3Mapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
	//the mapper sends phevBlended as key and city08 as value (average city MPG).
	private Text ResultKey = new Text();
	private FloatWritable ResultValue = new FloatWritable();
    public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
      String line = value.toString();
	  String[] values = line.split(",");
	  String phevBlended = values[49];
	  String city08 = values[4];
	  if (!phevBlended.matches("-?\\d+(\\.\\d+)?") && city08.matches("-?\\d+(\\.\\d+)?")) {//make sure phevBlended is not numerical and city08 is numerical.
      if (phevBlended != null && !phevBlended.isEmpty()) {//make sure phevBlended and city08 are not empty or null.
        if (city08 != null && !city08.isEmpty()) {ResultKey.set(phevBlended);ResultValue.set(Float.parseFloat(city08));context.write(ResultKey, ResultValue);}
      }
	  }
    }
}

class Program3Reducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	//Reducer produce the average of values just for records that have true as their keys (electrical cars).
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
	  String phevBlended =key.toString();
	  if(phevBlended.equals("true")){
		  float count = 0;
		  float sum = 0;
		  for (FloatWritable city08 : values) {
			count++;
			sum+=city08.get();
		  }
		  float ave=sum/count;
		  Text ResultKey = new Text("The average of city MPG just for electric cars:");
		  FloatWritable ResultValue =new FloatWritable(ave);
		  context.write(ResultKey, ResultValue);
	  }
    }
}
public class Reza_Marzban_Program_3{
  public static void main(String[] args) throws Exception{
	if(args.length!=2){
		System.err.println("Usage: Reza_Marzban_Program_3 <input path> <output path>");
		System.exit(-1);
	}
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Reza_Marzban_Program_3.class);
	job.setJobName("Reza Marzban Program 3");
    job.setMapperClass(Program3Mapper.class);
    job.setReducerClass(Program3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}