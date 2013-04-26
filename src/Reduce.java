import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	public static enum ResidualCounter {
        RESIDUE
	}
	
	boolean checkconvergence(Hashtable<Long,Double> tpr, Hashtable<Long,Double> cpr) {
		Enumeration<Long> enumKey = cpr.keys();
		double residual = 0;
		double val;
		long cnt = 0;
		while(enumKey.hasMoreElements()) {
		    Long key = enumKey.nextElement();
		    val = Math.abs(tpr.get(key).doubleValue() - cpr.get(key).doubleValue());
		    val /= cpr.get(key).doubleValue();
		    residual += val;
		    cnt++;
		}
		residual /= cnt;
		if(residual <= 0.001) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Hashtable<Long, Double> tpr = new Hashtable<Long,Double>(); //Prev iteration values
		Hashtable<Long, Double> ppr = new Hashtable<Long, Double>(); //Prev pass values
		Hashtable<Long, Double> cpr = new Hashtable<Long, Double>(); //Current iteration values
		Hashtable<Long, Double> degree = new Hashtable<Long, Double>();
		Hashtable<Long, String> info = new Hashtable<Long,String>();
		String[] parts = null;
		
		for(Text val : values) {
			if(val.charAt(0) == 'p') {
				//Page rank computation values
				parts = val.toString().split(";");
				ppr.put(new Long(parts[2]), new Double(Double.parseDouble(parts[3])));
				tpr.put(new Long(parts[2]), new Double(Double.parseDouble(parts[3])));
				degree.put(new Long(parts[2]), new Double(Double.parseDouble(parts[4])));
			} else {
				//Information
				parts = val.toString().split(";");
				info.put(new Long(parts[1]), parts[2]);
				ppr.put(new Long(parts[1]), new Double(Double.parseDouble(parts[2].split(" ")[0])) );
				tpr.put(new Long(parts[1]), new Double(Double.parseDouble(parts[2].split(" ")[0])) );
				degree.put(new Long(parts[1]), new Double(parts[2].split(" ").length-1));
			}
		}
		double division = 0;
		boolean first = true;
		Enumeration<Long> enumKey;
		do {
			if(!first) {
				enumKey = cpr.keys();
				while(enumKey.hasMoreElements()) {
				    Long keyv = enumKey.nextElement();
				    tpr.put(keyv,cpr.get(keyv));
				}
			}
			for(Text val : values) {
				if(val.charAt(0) == 'p') {
					parts = val.toString().split(";");
					division = tpr.get(parts[2]).doubleValue() / degree.get(parts[2]).doubleValue();
					if(cpr.get(parts[1]) != null) {
						cpr.put(new Long( parts[1] ), new Double( cpr.get(parts[1]).doubleValue() + division  ));
					} else {
						cpr.put(new Long( parts[1] ), new Double( division ));
					}
				}
			}
			enumKey = cpr.keys();
			while(enumKey.hasMoreElements()) {
			    Long keyv = enumKey.nextElement();
			    cpr.put(keyv, (0.15/PageRank.nodes) + (0.85 * cpr.get(keyv).doubleValue()) );
			}
			first = false;
			
		} while(checkconvergence(tpr,cpr));
		
		enumKey = cpr.keys();
		double residual = 0;
		double val;
		while(enumKey.hasMoreElements()) {
		    Long keyv = enumKey.nextElement();
		    val = Math.abs(cpr.get(keyv).doubleValue() - ppr.get(keyv).doubleValue());
		    val /= cpr.get(keyv).doubleValue();
		    residual += val;
		    
		    context.write(key, new Text(keyv+" "+cpr.get(keyv)+" "+degree.get(keyv)+ " " + info.get(keyv).substring((info.get(keyv).indexOf(" ")+1))) );
		}
		System.out.println("Residual:" + residual);
		residual *= PageRank.multiplication_factor; //To map residual to the long rank
		context.getCounter(ResidualCounter.RESIDUE).increment((long)residual);
	}
}