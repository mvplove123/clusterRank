package cluster.utils;

public class TGraph {
	
	long obj; //me
	long obj2; //dictree
	//int obj3; //encodingconvertor
	
	static {
		System.loadLibrary("segment");
	}
	
	public boolean open(DicTree dictree) {
		return open(dictree.obj);
	}
	
	
	public native boolean open(long dictree);
	
	public native int graphSegment(byte text[], int len, byte results[]);
	public native int segmentStr(byte text[], int len, byte results[]);

	public native void nameRec(boolean n);
	
	public native void deinit();
	
	public void finalnize() {
		deinit();
	}
	
	public static void main(String args[]) {
		DicTree dic = new DicTree("E:\\Search\\wordsegment\\trunk\\windows\\SegmentTest\\dicmap.bin");
		TGraph graph = new TGraph();
		graph.open(dic);
		String text = args[0];
		byte res[]=new byte[65536];


		byte src[] = text.getBytes();
		//System.out.println( src.length + " " + new String( src ) );
		int len = graph.segmentStr(src,src.length,res);
		
		System.out.println(text+"("+len+")="+new String(res,0,len));
	}
}
