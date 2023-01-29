/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.hdfs.cache;

import alluxio.client.file.CacheContext;
import alluxio.client.file.URIStatus;
import alluxio.hadoop.LocalCacheFileSystem;
import alluxio.wire.FileInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CachingFileSystem
        extends FileSystem
{
    private static final int BUFFER_SIZE = 65536;

    private final FileSystem dataTier;

    private LocalCacheFileSystem localCacheFileSystem;

    public CachingFileSystem(FileSystem dataTier)
    {
        this.dataTier = requireNonNull(dataTier, "dataTier is null");
    }

    @Override
    public synchronized void initialize(URI uri, Configuration configuration)
            throws IOException
    {
        this.localCacheFileSystem = new LocalCacheFileSystem(dataTier, uriStatus -> {
            try {
                return dataTier.open(new Path(uriStatus.getPath()));
            }
            catch (Exception e) {
                throw new IOException("Failed to open file", e);
            }
        });
        localCacheFileSystem.initialize(uri, configuration);
    }

    @Override
    public URI getUri()
    {
        return dataTier.getUri();
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        return dataTier.open(path, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
            int bufferSize, short replication,
            long blockSize, Progressable progressable)
            throws IOException
    {
        return dataTier.create(path, fsPermission, overwrite, bufferSize,
                replication, blockSize, progressable);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable)
            throws IOException
    {
        return dataTier.append(path, bufferSize, progressable);
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException
    {
        return dataTier.rename(src, dst);
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        return dataTier.delete(path, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws FileNotFoundException, IOException
    {
        return dataTier.listStatus(path);
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        dataTier.setWorkingDirectory(path);
    }

    @Override
    public Path getWorkingDirectory()
    {
        return dataTier.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission)
            throws IOException
    {
        return dataTier.mkdirs(path, fsPermission);
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return dataTier.getFileStatus(path);
    }

    public FSDataInputStream open(Path path, FileStatus status)
            throws IOException
    {
        FileInfo info = new FileInfo()
                .setLastModificationTimeMs(status.getModificationTime())
                .setPath(path.toString())
                .setFolder(false)
                .setLength(status.getLen());
        String cacheIdentifier = md5().hashString(path.toString(), UTF_8).toString();
        URIStatus uriStatus = new URIStatus(info, CacheContext.defaults().setCacheIdentifier(cacheIdentifier));
        return localCacheFileSystem.open(uriStatus, BUFFER_SIZE);
    }
}
