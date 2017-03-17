/*

 * Create

 */

package cluster.utils;



public class DicTree {

	long obj; //me

	long obj2; //encodingconvertor

	

	static {

		System.loadLibrary("segment");

	}	

	

	public DicTree(String path) {

		System.out.println("bingo0");
		byte text[] = path.getBytes();

		open(text, text.length);
		System.out.println("bingo1");

	}

	

	public native boolean open(byte text[], int len);

	public native void deinit();

	

	public void finalnize() {

		deinit();

	}

}

