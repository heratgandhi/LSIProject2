import java.io.*;

public class CreateInputFile {
	static boolean selectInputLine(double x,double rejectMin,double rejectLimit) {
		return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
	}
	public static void main(String[] args) {
		double fromNetID = 0.82;
		double rejectMin = 0.99 * fromNetID;
		double rejectLimit = rejectMin + 0.01;
		
		System.out.println(rejectMin + " " + rejectLimit);
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("edges.txt"));
			BufferedWriter bw = new BufferedWriter(new FileWriter("edges2.txt"));
			
			double counter = 1;
			String line = "";
			String lno = "";
			String rno = "";
			while((line = br.readLine())!=null) {
				
				counter = Double.parseDouble(line.substring(line.lastIndexOf(" ")));
				if(selectInputLine(counter, rejectMin, rejectLimit)) {
					line = line.substring(0,line.lastIndexOf(" "));
					lno = line.substring(0,6);
					rno = line.substring(7);
					lno = lno.trim();
					rno = rno.trim();
					bw.write(lno+" "+rno+"\n");
				}
				
			}
			br.close();
			bw.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
