package cluster.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
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





    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job)
            throws FileAlreadyExistsException,
            InvalidJobConfException, IOException {
        // Ensure that the output directory is set and not already there
        Path outDir = getOutputPath(job);
        if (outDir == null && job.getNumReduceTasks() != 0) {
            throw new InvalidJobConfException("Output directory not set in JobConf.");
        }
        if (outDir != null) {
            FileSystem fs = outDir.getFileSystem(job);
            // normalize the output directory
            outDir = fs.makeQualified(outDir);
            setOutputPath(job, outDir);

            // get delegation token for the outDir's file system
            TokenCache.obtainTokensForNamenodes(job.getCredentials(),
                    new Path[] {outDir}, job);

            // check its existence

        }
    }




}
