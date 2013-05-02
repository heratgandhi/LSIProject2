import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	
	long getBlock(long node) {
		return node%68;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		
		String[] parts1 = line.split("\t");
		
		String[] parts = parts1[1].split(" ");
		String source = parts[0];		
		String rank = parts[1];
		
		int deg = Integer.parseInt(parts[2]);
		if(deg != 0) {
			String list_v = rank;
			for(int i=0;i<deg;i++) {
				if(list_v == "") {
					list_v += parts[3+i];
				} else {
					list_v += " " + parts[3+i];
				}
				//System.out.println(blockNo(Long.parseLong(parts[3+i]), blocklimits));
				//System.out.println(new Text(blockNo(Long.parseLong(parts[3+i]), blocklimits)+"")+" " + new Text("p;"+parts[3+i]+";"+source+";"+rank+";"+deg));//division))
				context.write(new Text(getBlock(Long.parseLong(parts[3+i]))+""), new Text("p;"+parts[3+i]+";"+source+";"+rank+";"+deg));//division));
			}
			//System.out.println(new Text(source_b)+" "+new Text("i;"+source+";"+list_v));
			context.write(new Text(getBlock(Long.parseLong(source))+""), new Text("i;"+source+";"+list_v));
		} else {
			context.write(new Text(getBlock(Long.parseLong(source))+""), new Text("i;"+source+"; "+rank+" 0"));
		}
		
	}
}