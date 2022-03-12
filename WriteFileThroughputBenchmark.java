package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@CommandLine.Command(name = "write-throughput-benchmark",
        aliases = "wtb",
        description = "Benchmark for creating a file",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class WriteFileThroughputBenchmark extends BaseFreonGenerator implements Callable<Void>{

    @Option(names = {"-o"},
            description = "Ozone filesystem path",
            defaultValue = "o3fs://bucket1.vol1")
    private String rootPath;

    @Option(names = {"-s", "--size"},
            description = "Size of each generated files (in GB)",
            defaultValue = "1")
    private long fileSize;

    @Option(names = {"-b", "--block"},
            description = "Specify the Block Size in MB",
            defaultValue = "128")
    private long blockSize;

    @Option(names = {"-i", "--buffer"},
            description = "Size of buffer used store the generated key content",
            defaultValue = "10240")
    private int bufferSize;

    @Option(names = {"-th", "--throttle"},
            description = "Specify the Delay in Input/Output",
            defaultValue = "0")
    private int throttle;

    @Option(names = {"-re", "--replication"},
            description = "Specify the Replication factor",
            defaultValue = "3")
    private short replication;

    @Option(names= {"--sync"},
            description = "Optionally Issue hsync after every write Cannot be used with hflush",
            defaultValue = "false"
    )
    private boolean hSync;

    @Option(names= {"--flush"},
            description = "Optionally Issue hsync after every write Cannot be used with hflush",
            defaultValue = "false"
    )
    private boolean hFlush;

    // For Generating the content of the files
    private ContentGenerator contentGenerator;
    // For Creating the required configurations for the file system
    private OzoneConfiguration configuration;

    private URI uri;
    private boolean isThrottled;

    public static final Logger LOG =
            LoggerFactory.getLogger(WriteFileThroughputBenchmark.class);

    // Checking whether an output directory is created inside the bucket
    private static void ensureOutputDirExists(FileSystem fs, Path outputDir)
            throws IOException {
        if (fs.exists(outputDir)) {
            LOG.error("No Such Output Directory exists : {}", outputDir);
            System.exit(1);
        }
    }


    public Void call() throws Exception{

        // Initialize the configuration variable
        configuration = createOzoneConfiguration();

        //Constructs a URI by parsing the given string rootPath
        // We Initialize the uri variable with the path
        uri = URI.create(rootPath);

        LOG.info("NumFiles=" + getTestNo());
        LOG.info("Total FileSize=" + fileSize);
        LOG.info("BlockSize=" + blockSize);
        LOG.info("BufferSize=" + bufferSize);
        LOG.info("Replication=" + replication);
        LOG.info("Threads=" + getThreadNo());
        LOG.info("URI Scheme Used=" + uri.getScheme());
        if(hSync){
            LOG.info("Hsync after every write= True");
        }
        else if(hFlush){
            LOG.info("Hflush after every write= True");
        }

        // Create an object which accesses the filesystem API
        FileSystem fileSystem = createFS();

        // Creating a thread pool of 10 threads
        ExecutorService service = Executors.newFixedThreadPool(10);

        // Code for handling the throttle delay
        if(throttle > 0) isThrottled = true;
        final byte[] data = new byte[bufferSize];
        final long expectedIoTimeNs =
                (isThrottled ? (((long) data.length * 1_000_000_000) / throttle)
                        : 0);

        for (int i=0;i<10;i++){
        service.execute(new Runnable() {
            // Override the run method
            public void run()
            {
                try {
                    createFile(fileSystem,data,expectedIoTimeNs);
                } catch (Exception e) {
                    LOG.info("Stack Trace: {0}", e);
                }
            }
        });
        }
        service.shutdown();
        fileSystem.close();
        return null;
    }

    private void createFile(FileSystem fileSystem, byte[] data, long expectedIoTimeNs) throws Exception {
        // generateObjectName is taken from the BaseFreonGenerator class
        // file parameter will have the path as well as the name of the file to be written
        Path file = new Path(rootPath + "/" + generateObjectName(0));

        // Checks if output directory is available
        ensureOutputDirExists(fileSystem,file);

        // Initialize the size of the file to be written
        long filesizeinBytes = fileSize*1_000_000_000;
        contentGenerator =  new ContentGenerator(filesizeinBytes, bufferSize, hSync, hFlush);

        final long ioStartTimeNs = (isThrottled ? System.nanoTime() : 0);

        FSDataOutputStream outputStream = fileSystem.create(file,false,bufferSize,replication,blockSize);
        contentGenerator.write(outputStream);

        final long ioEndTimeNs = (isThrottled ? System.nanoTime() : 0);
        enforceThrottle(ioEndTimeNs - ioStartTimeNs, expectedIoTimeNs);

    }

    private FileSystem createFS() {
        try {
            return FileSystem.get(uri, configuration);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    static void enforceThrottle(long ioTimeNs, long expectedIoTimeNs)
            throws InterruptedException {
        if (ioTimeNs < expectedIoTimeNs) {
            // The IO completed too fast, so sleep for some time.
            long sleepTimeNs = expectedIoTimeNs - ioTimeNs;
            Thread.sleep(sleepTimeNs / 1_000_000, (int) (sleepTimeNs % 1_000_000));
        }
    }
}
