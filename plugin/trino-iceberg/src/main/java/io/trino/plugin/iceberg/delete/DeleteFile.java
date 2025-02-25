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
package io.trino.plugin.iceberg.delete;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.openjdk.jol.info.ClassLayout;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public final class DeleteFile
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DeleteFile.class).instanceSize();

    private final FileContent content;
    private final String path;
    private final FileFormat format;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final List<Integer> equalityFieldIds;
    private final Map<Integer, byte[]> lowerBounds;
    private final Map<Integer, byte[]> upperBounds;

    public static DeleteFile fromIceberg(org.apache.iceberg.DeleteFile deleteFile)
    {
        Map<Integer, byte[]> lowerBounds = firstNonNull(deleteFile.lowerBounds(), ImmutableMap.<Integer, ByteBuffer>of())
                .entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().array().clone()));
        Map<Integer, byte[]> upperBounds = firstNonNull(deleteFile.upperBounds(), ImmutableMap.<Integer, ByteBuffer>of())
                .entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().array().clone()));

        return new DeleteFile(
                deleteFile.content(),
                deleteFile.path().toString(),
                deleteFile.format(),
                deleteFile.recordCount(),
                deleteFile.fileSizeInBytes(),
                Optional.ofNullable(deleteFile.equalityFieldIds()).orElseGet(ImmutableList::of),
                lowerBounds,
                upperBounds);
    }

    @JsonCreator
    public DeleteFile(
            FileContent content,
            String path,
            FileFormat format,
            long recordCount,
            long fileSizeInBytes,
            List<Integer> equalityFieldIds,
            Map<Integer, byte[]> lowerBounds,
            Map<Integer, byte[]> upperBounds)
    {
        this.content = requireNonNull(content, "content is null");
        this.path = requireNonNull(path, "path is null");
        this.format = requireNonNull(format, "format is null");
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.equalityFieldIds = ImmutableList.copyOf(requireNonNull(equalityFieldIds, "equalityFieldIds is null"));
        this.lowerBounds = ImmutableMap.copyOf(requireNonNull(lowerBounds, "lowerBounds is null"));
        this.upperBounds = ImmutableMap.copyOf(requireNonNull(upperBounds, "upperBounds is null"));
    }

    @JsonProperty
    public FileContent content()
    {
        return content;
    }

    @JsonProperty
    public CharSequence path()
    {
        return path;
    }

    @JsonProperty
    public FileFormat format()
    {
        return format;
    }

    @JsonProperty
    public long recordCount()
    {
        return recordCount;
    }

    @JsonProperty
    public long fileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    @JsonProperty
    public List<Integer> equalityFieldIds()
    {
        return equalityFieldIds;
    }

    @JsonProperty
    public Map<Integer, byte[]> getLowerBounds()
    {
        return lowerBounds;
    }

    @JsonProperty
    public Map<Integer, byte[]> getUpperBounds()
    {
        return upperBounds;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(equalityFieldIds, ignored -> SIZE_OF_INT)
                + estimatedSizeOf(lowerBounds, entry -> SIZE_OF_INT, SizeOf::sizeOf)
                + estimatedSizeOf(upperBounds, entry -> SIZE_OF_INT, SizeOf::sizeOf);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .add("records", recordCount)
                .toString();
    }
}
