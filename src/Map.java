import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		String[] parts1 = line.split("\t");
		String source = parts1[0];
		String[] parts = parts1[1].split(" ");
		String rank = parts[0];
		
		int deg = Integer.parseInt(parts[1]);
		
		String division = (Double.parseDouble(rank) / deg) + "";
		
		String list_v = rank;
		for(int i=0;i<deg;i++) {
			if(list_v == "") {
				list_v += parts[2+i];
			} else {
				list_v += " " + parts[2+i];
			}			
			context.write(new Text(parts[2+i]), new Text(division));
		}
		context.write(new Text(source), new Text(list_v));
	}
}