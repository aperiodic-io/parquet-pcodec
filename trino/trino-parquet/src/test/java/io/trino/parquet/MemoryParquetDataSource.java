/*
 * tparquet fork (trino/trino-parquet/FORK.md): new file.
 */
package io.trino.parquet;

import static java.lang.Math.toIntExact;

/** A Parquet file in memory, for the fork's tests. */
public final class MemoryParquetDataSource
        extends AbstractParquetDataSource
{
    private final byte[] data;

    public MemoryParquetDataSource(byte[] data)
    {
        super(new ParquetDataSourceId("memory"), data.length, ParquetReaderOptions.defaultOptions());
        this.data = data;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        System.arraycopy(data, toIntExact(position), buffer, bufferOffset, bufferLength);
    }
}
