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


class Program4Mapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
	//the mapper sends phevBlended as key and barrels08 as value (annual petroleum consumption).
	private Text ResultKey = new Text();
	private FloatWritable ResultValue = new FloatWritable();
    public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
      String line = value.toString();
	  String[] values = line.split(",");
	  String phevBlended = values[49];
	  String barrels08 = values[0];
	  if (!phevBlended.matches("-?\\d+(\\.\\d+)?") && barrels08.matches("-?\\d+(\\.\\d+)?")) {//make sure phevBlended is not numerical and barrels08 is numerical.
      if (phevBlended != null && !phevBlended.isEmpty()) {//make sure phevBlended and barrels08 are not empty or null.
        if (barrels08 != null && !barrels08.isEmpty()) {ResultKey.set(phevBlended);ResultValue.set(Float.parseFloat(barrels08));context.write(ResultKey, ResultValue);}
      }
	  }
    }
}

class Program4Reducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	//Reducer produce the average of values for Electrical cars and gasoline cars. 
     float[] result = new float[2];
	 int counter=0;
	 public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
	  String phevBlended =key.toString();
	  if(phevBlended.equals("true") || phevBlended.equals("false") ){
		  float count = 0;
		  float sum = 0;
		  for (FloatWritable city08 : values) {
			count++;
			sum+=city08.get();
		  }
		  float ave=sum/count;
		  result[counter]=ave;
		  counter++;
	  }
    }
	protected void cleanup(Context context) throws IOException, InterruptedException {
	//calculate the difference between average of petroleum consumption of electric cars and gasoline cars. 
		float dif=Math.abs(result[0]-result[1]);
		Text ResultKey = new Text("The difference of petroleum consumption of electric cars and gasoline cars:");
		FloatWritable ResultValue =new FloatWritable(dif);
		context.write(ResultKey, ResultValue);
    }
	
}
public class Reza_Marzban_Program_4{
  public static void main(String[] args) throws Exception{
	if(args.length!=2){
		System.err.println("Usage: Reza_Marzban_Program_4 <input path> <output path>");
		System.exit(-1);
	}
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Reza_Marzban_Program_4.class);
	job.setJobName("Reza Marzban Program 4");
    job.setMapperClass(Program4Mapper.class);
    job.setReducerClass(Program4Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}