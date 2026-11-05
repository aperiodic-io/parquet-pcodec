package io.aperiodic.tparquet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;

/** How a file's pages are stored: for each column, the distinct "PAGE_TYPE:MODE" of its pages, read from the raw page headers ("-" for an uncompressed V2 page or a codec other than PCODEC). */
public final class Inspect {
  private Inspect() {}

  public static Map<String, List<String>> modes(Path file) throws IOException {
    byte[] bytes = Files.readAllBytes(file);
    Map<String, List<String>> out = new LinkedHashMap<>();
    try (ParquetFileReader r = ParquetFileReader.open(new LocalInputFile(file))) {
      for (BlockMetaData block : r.getFooter().getBlocks()) {
        for (ColumnChunkMetaData chunk : block.getColumns()) {
          long pos = chunk.getStartingPos();
          long end = pos + chunk.getTotalSize();
          while (pos < end) {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes, (int) pos, (int) (end - pos));
            int before = in.available();
            PageHeader h = Util.readPageHeader(in);
            long body = pos + (before - in.available());
            int levels = h.getType() == PageType.DATA_PAGE_V2
                ? h.getData_page_header_v2().getRepetition_levels_byte_length() + h.getData_page_header_v2().getDefinition_levels_byte_length()
                : 0;
            boolean compressed = h.getType() != PageType.DATA_PAGE_V2 || h.getData_page_header_v2().isIs_compressed();
            String mode = "-";
            if (compressed && chunk.getCodec() == CompressionCodecName.PCODEC) {
              mode = Pcodec.mode(Arrays.copyOfRange(bytes, (int) (body + levels), (int) (body + h.getCompressed_page_size()))).name();
            }
            List<String> l = out.computeIfAbsent(chunk.getPath().toDotString(), k -> new ArrayList<>());
            if (!l.contains(h.getType() + ":" + mode)) {
              l.add(h.getType() + ":" + mode);
            }
            pos = body + h.getCompressed_page_size();
          }
        }
      }
    }
    return out;
  }
}
