import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	public static enum ResidualCounter {
        RESIDUE
	}
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		double sum = 0;
		String list = "";
		double prank = 0;
		double residue = 0;
		double rank = 0;
		int deg = 0;
		for(Text val : values) {
			try{
				sum += Double.parseDouble(val.toString());				
			} catch(Exception e) { 
				list = val.toString();
				prank = Double.parseDouble(list.split(" ")[0]);
				list = list.substring(list.indexOf(" ")+1);
				deg = list.split(" ").length;
			}
		}
		rank = ((0.15)/PageRank.nodes) + (0.85 * sum);
		rank = (double) Math.round(rank * 10000) / 10000;
		
		residue = Math.abs(rank - prank) / rank;
		residue = (double) Math.round(residue * 10000) / 10000;
		residue *= PageRank.multiplication_factor; //To map residual to the long rank
		
		context.getCounter(ResidualCounter.RESIDUE).increment((long)residue);
		
		System.out.println(key.toString()+" "+residue+" "+rank);
		
		context.write(key, new Text(rank+" "+deg+" "+list));
	}
}