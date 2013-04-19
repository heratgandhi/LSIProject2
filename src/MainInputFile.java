import java.io.*;

public class MainInputFile {
	public static void main(String[] args) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("edges1.txt"));
			BufferedWriter bw = new BufferedWriter(new FileWriter("input"));
			
			String line = "";
			long counter = 0;
			long node = 0;
			String[] parts;
			String dest = "";
			long s,d;
			while ( (line=br.readLine()) != null ) {
				parts = line.split(" ");
				s = Long.parseLong(parts[0]);
				d = Long.parseLong(parts[1]);
				if(s == node) {
					if(counter != 0)
						dest += " " + d;
					else
						dest += d;
					counter ++;
				} else {
					bw.write(node+" 1 "+counter+" "+dest+"\n");
					dest = d+"";
					node++;
					counter = 1;
				}
			}
			bw.write(node+" 1 "+counter+" "+dest+"\n");
			br.close();
			bw.close();
		} catch(Exception e) {
			
		}
	}
}
