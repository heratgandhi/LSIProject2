import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	
	int blockNo(long node,long[] blocklimits) {
		for(int i=0;i<blocklimits.length;i++) {
			if(node < blocklimits[i]) {
				return i;
			}
		}
		return -1;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		long[] blocklimits = new long[68];
		blocklimits[0] = 10328;
		blocklimits[1] = 20373;
		blocklimits[2] = 30629;
		blocklimits[3] = 40645;
		blocklimits[4] = 50462;
		blocklimits[5] = 60841;
		blocklimits[6] = 70591;
		blocklimits[7] = 80118;
		blocklimits[8] = 90497;
		blocklimits[9] = 100501;
		blocklimits[10] = 110567;
		blocklimits[11] = 120945;
		blocklimits[12] = 130999;
		blocklimits[13] = 140574;
		blocklimits[14] = 150953;
		blocklimits[15] = 161332;
		blocklimits[16] = 171154;
		blocklimits[17] = 181514;
		blocklimits[18] = 191625;
		blocklimits[19] = 202004;
		blocklimits[20] = 212383;
		blocklimits[21] = 222762;
		blocklimits[22] = 232593;
		blocklimits[23] = 242878;
		blocklimits[24] = 252938;
		blocklimits[25] = 263149;
		blocklimits[26] = 273210;
		blocklimits[27] = 283473;
		blocklimits[28] = 293255;
		blocklimits[29] = 303043;
		blocklimits[30] = 313370;
		blocklimits[31] = 323522;
		blocklimits[32] = 333883;
		blocklimits[33] = 343663;
		blocklimits[34] = 353645;
		blocklimits[35] = 363929;
		blocklimits[36] = 374236;
		blocklimits[37] = 384554;
		blocklimits[38] = 394929;
		blocklimits[39] = 404712;
		blocklimits[40] = 414617;
		blocklimits[41] = 424747;
		blocklimits[42] = 434707;
		blocklimits[43] = 444489;
		blocklimits[44] = 454285;
		blocklimits[45] = 464398;
		blocklimits[46] = 474196;
		blocklimits[47] = 484050;
		blocklimits[48] = 493968;
		blocklimits[49] = 503752;
		blocklimits[50] = 514131;
		blocklimits[51] = 524510;
		blocklimits[52] = 534709;
		blocklimits[53] = 545088;
		blocklimits[54] = 555467;
		blocklimits[55] = 565846;
		blocklimits[56] = 576225;
		blocklimits[57] = 586604;
		blocklimits[58] = 596585;
		blocklimits[59] = 606367;
		blocklimits[60] = 616148;
		blocklimits[61] = 626448;
		blocklimits[62] = 636240;
		blocklimits[63] = 646022;
		blocklimits[64] = 655804;
		blocklimits[65] = 665666;
		blocklimits[66] = 675448;
		blocklimits[67] = 685230;
		
		/*long[] blocklimits = new long[3];
		blocklimits[0] = 1;
		blocklimits[1] = 5;
		blocklimits[2] = 6;*/
		
		String[] parts1 = line.split("\t");
		String source_b = parts1[0];
		
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
				context.write(new Text(blockNo(Long.parseLong(parts[3+i]), blocklimits)+""), new Text("p;"+parts[3+i]+";"+source+";"+rank+";"+deg));//division));
			}
			//System.out.println(new Text(source_b)+" "+new Text("i;"+source+";"+list_v));
			context.write(new Text(source_b), new Text("i;"+source+";"+list_v));
		} else {
			context.write(new Text(source_b), new Text("i;"+source+"; "+rank+" 0"));
		}
		
	}
}