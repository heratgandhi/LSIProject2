import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		/*int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
		*/
		double sum = 0;
		String list = "";
		int deg = 0;
		for(Text val : values) {
			try{
				sum += Double.parseDouble(val.toString());
				
			} catch(Exception e) { 
				//context.write(key, val);
				list = val.toString();
				deg = list.split(" ").length;
			}
		}
		context.write(key, new Text(sum+" "+deg+" "+list));
	}
}