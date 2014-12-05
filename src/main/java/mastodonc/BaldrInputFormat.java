package mastodonc;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class BaldrInputFormat extends FileInputFormat {
    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter) throws IOException {
        return new BaldrRecordReader((FileSplit)inputSplit, conf);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    // http://grepcode.com/file/repo1.maven.org/maven2/org.jvnet.hudson.hadoop/hadoop-core/0.19.1-hudson-2/org/apache/hadoop/mapred/LineRecordReader.java#LineRecordReader

    private class BaldrRecordReader implements RecordReader<LongWritable, BytesWritable> {
        private final BaldrReader reader;
        private final long end;
        private final long start;
        private long position = 0;

        public BaldrRecordReader(FileSplit inputSplit, JobConf conf) throws IOException {

            // TODO
            // figure out if we need to read from start...
            start = inputSplit.getStart();
            end = start + inputSplit.getLength();

            final Path file = inputSplit.getPath();

            CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
            CompressionCodec codec = codecFactory.getCodec(file);

            FileSystem fileSystem = file.getFileSystem(conf);
            FSDataInputStream underlyingStream = fileSystem.open(inputSplit.getPath());

            if (codec != null) {
                // open stream via codec
                CompressionInputStream stream = codec.createInputStream(underlyingStream);
                reader = new BaldrReader(stream);
            } else {
                // open stream directly from underlyingStream
                reader = new BaldrReader(underlyingStream);
            }
        }

        @Override
        public boolean next(LongWritable offset, BytesWritable bytes) throws IOException {
            BaldrRecord record = reader.next();

            if (record == null) {
                // end of file
                return false;
            }
            this.position = record.position();
            offset.set(record.position());
            bytes.set(record.bytes(), 0, record.length());

            return true;
        }

        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public BytesWritable createValue() {
            return new BytesWritable();
        }

        @Override
        // return current position in stream
        public long getPos() throws IOException {
            return this.position;
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }

        @Override
        // progress in percentage
        public float getProgress() throws IOException {
            return Math.min(1.0f, (this.getPos() - start) / (float)(end - start));
        }
    }
}
