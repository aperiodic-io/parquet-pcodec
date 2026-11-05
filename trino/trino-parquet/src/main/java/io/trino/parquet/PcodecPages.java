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
package io.trino.parquet;

import io.airlift.slice.Slice;
import io.aperiodic.tparquet.Pcodec;
import io.aperiodic.tparquet.PcodecException;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;

/**
 * tparquet fork (trino/trino-parquet/FORK.md): new file. Decompresses the pages of PCODEC column
 * chunks (spec/FORMAT.md, sections 3 and 4) for {@link ParquetCompressionUtils#decompress}.
 */
final class PcodecPages
{
    private PcodecPages() {}

    /**
     * Decodes a PCODEC payload to exactly {@code uncompressedSize} bytes. Payloads are untrusted: a
     * malformed one, or one that decodes to another size, is a {@link ParquetCorruptionException},
     * as is a negative size. As the reference reader does, a page with nothing to decode (a V2 page
     * of nulls) is not decoded; an empty payload with bytes expected is malformed.
     */
    static Slice decompress(ParquetDataSourceId dataSourceId, Slice input, int uncompressedSize)
            throws ParquetCorruptionException
    {
        if (uncompressedSize < 0) {
            throw new ParquetCorruptionException(dataSourceId, "Invalid PCODEC page: negative uncompressed size %s", uncompressedSize);
        }
        if (uncompressedSize == 0) {
            return EMPTY_SLICE;
        }
        byte[] output = new byte[uncompressedSize];
        try {
            Pcodec.decompress(input.byteArray(), input.byteArrayOffset(), input.length(), output, 0, uncompressedSize);
        }
        catch (PcodecException e) {
            throw new ParquetCorruptionException(e, dataSourceId, "Invalid PCODEC page: %s", e.getMessage());
        }
        return wrappedBuffer(output);
    }
}
