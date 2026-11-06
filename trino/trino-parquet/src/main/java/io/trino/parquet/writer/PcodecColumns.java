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
package io.trino.parquet.writer;

import io.aperiodic.tparquet.Pcodec;
import io.aperiodic.tparquet.PhysicalType;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.parquet.writer.ParquetDataOutput.createDataOutput;

/**
 * tparquet fork (trino/trino-parquet/FORK.md): new file. PCODEC in Trino's writer, as spec/FORMAT.md
 * sections 2 and 3 recommend, within what Trino's writer does (DATA_PAGE pages, one codec per
 * file): the columns whose pages are arrays of numbers get PLAIN values without a dictionary and
 * their physical type as the type hint; the others keep Trino's encodings, and their payloads are
 * the codec's ZSTD fallback (valid files, spec section 4).
 */
final class PcodecColumns
{
    private PcodecColumns() {}

    /**
     * The type hint of a column's pages: its physical type when the pages are arrays of numbers,
     * an INT32, INT64, FLOAT or DOUBLE column without definition or repetition levels (a DATA_PAGE
     * starts with its levels); {@code null} for the others.
     */
    @Nullable
    static PhysicalType hint(ColumnDescriptor column)
    {
        if (column.getMaxDefinitionLevel() != 0 || column.getMaxRepetitionLevel() != 0) {
            return null;
        }
        return PhysicalType.fromName(column.getPrimitiveType().getPrimitiveTypeName().name());
    }

    /** Whether the column's values are written PLAIN without a dictionary: a PCODEC column with a type hint. */
    static boolean plainValues(CompressionCodec codec, ColumnDescriptor column)
    {
        return codec == CompressionCodec.PCODEC && hint(column) != null;
    }

    /** {@code parquet.pcodec.version = "1"} for a file with a PCODEC column chunk (spec section 2). */
    static Optional<KeyValue> footerKeyValue(List<RowGroup> rowGroups)
    {
        boolean pcodec = rowGroups.stream()
                .flatMap(rowGroup -> rowGroup.getColumns().stream())
                .anyMatch(column -> column.getMeta_data().getCodec() == CompressionCodec.PCODEC);
        return pcodec ? Optional.of(new KeyValue(Pcodec.KV_KEY).setValue(Pcodec.KV_VALUE)) : Optional.empty();
    }

    /** PCODEC at the default level, trying DECIMAL for floats. */
    static final class PcodecCompressor
            implements ParquetCompressor
    {
        @Nullable
        private final PhysicalType hint;

        PcodecCompressor(@Nullable PhysicalType hint)
        {
            this.hint = hint;
        }

        @Nullable
        PhysicalType hint()
        {
            return hint;
        }

        @Override
        public ParquetDataOutput compress(ByteBuffer input)
        {
            byte[] page = new byte[input.remaining()];
            input.get(page);
            return createDataOutput(wrappedBuffer(Pcodec.compress(page, 0, page.length, hint, Pcodec.DEFAULT_LEVEL, true)));
        }
    }
}
