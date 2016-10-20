package cluster.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * Created by admin on 2016/10/20.
 */
public class GBKMultipleTextOutputFormat<K, V> extends MultipleOutputFormat<K, V> {

    private GBKTextOutputFormat<K, V> theTextOutputFormat = null;

    @Override
    protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs, JobConf job,
                                                     String name, Progressable arg3) throws IOException {
        if (theTextOutputFormat == null) {
            theTextOutputFormat = new GBKTextOutputFormat<K, V>();
        }
        return theTextOutputFormat.getRecordWriter(fs, job, name, arg3);
    }




}
