package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

public class MockFileSystem extends ChecksumFileSystem {
    static final URI NAME = URI.create("file:///");
    static private Random rand = new Random();

    public MockFileSystem() {
        this(new RawLocalFileSystem());
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        if (fs.getConf() == null) {
            fs.initialize(name, conf);
        }
        String scheme = name.getScheme();
        if (!scheme.equals(fs.getUri().getScheme())) {
            swapScheme = scheme;
        }
    }

    /**
     * Return the protocol scheme for the FileSystem.
     * <p/>
     *
     * @return <code>file</code>
     */
    @Override
    public String getScheme() {
        return "file";
    }

    public FileSystem getRaw() {
        return getRawFileSystem();
    }

    public MockFileSystem(FileSystem rawLocalFileSystem) {
        super(rawLocalFileSystem);
    }

    /**
     * Convert a path to a File.
     */
    public File pathToFile(Path path) {
        return ((RawLocalFileSystem) fs).pathToFile(path);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException {
        FileUtil.copy(this, src, this, dst, delSrc, getConf());
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException {
        FileUtil.copy(this, src, this, dst, delSrc, getConf());
    }

    /**
     * Moves files to a bad file directory on the same device, so that their
     * storage will not be reused.
     */
    @Override
    public boolean reportChecksumFailure(Path p, FSDataInputStream in,
                                         long inPos,
                                         FSDataInputStream sums, long sumsPos) {
        try {
            // canonicalize f
            File f = ((RawLocalFileSystem) fs).pathToFile(p).getCanonicalFile();

            // find highest writable parent dir of f on the same device
            String device = new DF(f, getConf()).getMount();
            File parent = f.getParentFile();
            File dir = null;
            while (parent != null && FileUtil.canWrite(parent) &&
                    parent.toString().startsWith(device)) {
                dir = parent;
                parent = parent.getParentFile();
            }

            if (dir == null) {
                throw new IOException(
                        "not able to find the highest writable parent dir");
            }

            // move the file there
            File badDir = new File(dir, "bad_files");
            if (!badDir.mkdirs()) {
                if (!badDir.isDirectory()) {
                    throw new IOException("Mkdirs failed to create " + badDir.toString());
                }
            }
            String suffix = "." + rand.nextInt();
            File badFile = new File(badDir, f.getName() + suffix);
            LOG.warn("Moving bad file " + f + " to " + badFile);
            in.close();                               // close it first
            boolean b = f.renameTo(badFile);                      // rename it
            if (!b) {
                LOG.warn("Ignoring failure of renameTo");
            }
            // move checksum file too
            File checkFile = ((RawLocalFileSystem) fs).pathToFile(getChecksumFile(p));
            // close the stream before rename to release the file handle
            sums.close();
            b = checkFile.renameTo(new File(badDir, checkFile.getName() + suffix));
            if (!b) {
                LOG.warn("Ignoring failure of renameTo");
            }
        } catch (IOException e) {
            LOG.warn("Error moving bad file " + p + ": " + e);
        }
        return false;
    }

    @Override
    public boolean supportsSymlinks() {
        return true;
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws IOException {
        fs.createSymlink(target, link, createParent);
    }

    @Override
    public FileStatus getFileLinkStatus(final Path f) throws IOException {
        return fs.getFileLinkStatus(f);
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
        return fs.getLinkTarget(f);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        throw new IOException("");
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        FileStatus f1 = new FileStatus(
                10L,
                false,
                0,
                1,
                101010L,
                new Path(getWorkingDirectory() + "/0")
        );
        FileStatus f2 = new FileStatus(
                10L,
                false,
                0,
                1,
                101010L,
                new Path(getWorkingDirectory() + "/1.compact")
        );

        return new FileStatus[]{f1, f2};
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException{
        throw new IOException("delete failure");
    }
}


