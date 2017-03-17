package cluster.utils;
//import com.go2map.util.DicTree;

import java.io.IOException;
import java.lang.reflect.Field;

public class WordSegmentor {
	
	long obj; //me
	long obj2; //dictree
	//int obj3; //encodingconvertor
	
	static {
		try {
			addLibraryDir("D:\\structure\\lib");
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.loadLibrary("segment");
	}
	
	public boolean open(DicTree dictree) {
		return open(dictree.obj);
	}
	
	
	public native boolean open(long dictree);
	
	public native int segment(byte text[], int len, byte results[], int flag, boolean synonymous, boolean withTermType);
	public native int simpleSegment(byte text[], int len, byte results[], boolean synonymous);
	public native int hintSegment(byte text[], int len, byte results[]);
	public native int complexSegment(byte text[], int len, byte results[]);
	
	public native void nameRec(boolean n);

	public native void deinit();

	public void finalnize() {
		deinit();
	}


	public static void main(String args[]) {


		DicTree dic = new DicTree("D:\\structure\\splitword\\dicmap.bin");// 加载词典文件
		WordSegmentor seg = new WordSegmentor();
		seg.open(dic);
		System.out.println("load dic sucess.");
        seg.nameRec(false);
		String text = "北京市海淀区五道口麦当劳";
		byte res[]=new byte[65536];
		
		byte src[] = text.getBytes();
		//System.out.println( src.length + " " + new String( src ) );
		int len = seg.segment(src, src.length, res, 0, true, true);//后面两个boolen类型的参数代表是否产生同义词，是否生成词性
		
		System.out.println(text + "("+len+")=" + new String(res,0,len));
	}

	public static void addLibraryDir(String libraryPath) throws IOException {
		try {
			Field field = ClassLoader.class.getDeclaredField("usr_paths");
			field.setAccessible(true);
			String[] paths = (String[]) field.get(null);
			for (int i = 0; i < paths.length; i++) {
				if (libraryPath.equals(paths[i])) {
					return;
				}
			}

			String[] tmp = new String[paths.length + 1];
			System.arraycopy(paths, 0, tmp, 0, paths.length);
			tmp[paths.length] = libraryPath;
			field.set(null, tmp);
		} catch (IllegalAccessException e) {
			throw new IOException(
					"Failedto get permissions to set library path");
		} catch (NoSuchFieldException e) {
			throw new IOException(
					"Failedto get field handle to set library path");
		}
	}

}
