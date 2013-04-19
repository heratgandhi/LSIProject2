import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		String[] parts = line.split(" ");
		String source = parts[0];
		String rank = parts[1];
		
		int deg = Integer.parseInt(parts[2]);
		
		String division = (Double.parseDouble(rank) / deg) + "";
		
		String list_v = "";
		for(int i=0;i<deg;i++) {
			list_v += parts[3+i];
			context.write(new Text(parts[3+i]), new Text(division));
		}
		context.write(new Text(source), new Text(list_v));
	}
}