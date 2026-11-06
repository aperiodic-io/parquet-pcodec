package org.apache.parquet.hadoop;

import io.aperiodic.tparquet.Pcodec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/** The pages of a file as stored: for each column, "PAGE_TYPE:MODE" per page (MODE "-" for an uncompressed V2 page). */
final class PcodecPages {
  /** Where one page's compressed bytes are in the file. */
  record Page(String column, PageType type, long payloadOffset, int payloadLength, String mode) {}

  private PcodecPages() {}

  static List<Page> pages(java.nio.file.Path file) throws IOException {
    byte[] bytes = Files.readAllBytes(file);
    ParquetMetadata footer;
    try (ParquetFileReader r = ParquetFileReader.open(new Configuration(), new Path(file.toUri()))) {
      footer = r.getFooter();
    }
    List<Page> out = new ArrayList<>();
    for (BlockMetaData block : footer.getBlocks()) {
      for (ColumnChunkMetaData chunk : block.getColumns()) {
        long pos = chunk.getStartingPos();
        long end = pos + chunk.getTotalSize();
        while (pos < end) {
          ByteArrayInputStream in = new ByteArrayInputStream(bytes, (int) pos, (int) (end - pos));
          int before = in.available();
          PageHeader h = Util.readPageHeader(in);
          long body = pos + (before - in.available());
          int levels = 0;
          boolean compressed = true;
          if (h.getType() == PageType.DATA_PAGE_V2) {
            levels = h.getData_page_header_v2().getRepetition_levels_byte_length()
                + h.getData_page_header_v2().getDefinition_levels_byte_length();
            compressed = h.getData_page_header_v2().isIs_compressed();
          }
          long payload = body + levels;
          int length = h.getCompressed_page_size() - levels;
          String mode = "-";
          if (compressed && chunk.getCodec() == CompressionCodecName.PCODEC) {
            byte[] p = java.util.Arrays.copyOfRange(bytes, (int) payload, (int) payload + length);
            mode = Pcodec.mode(p).name();
          }
          out.add(new Page(chunk.getPath().toDotString(), h.getType(), payload, length, mode));
          pos = body + h.getCompressed_page_size();
        }
      }
    }
    return out;
  }

  /** column -> the distinct "PAGE_TYPE:MODE" of its pages. */
  static Map<String, List<String>> modes(java.nio.file.Path file) throws IOException {
    Map<String, List<String>> m = new LinkedHashMap<>();
    for (Page p : pages(file)) {
      List<String> l = m.computeIfAbsent(p.column(), k -> new ArrayList<>());
      String s = p.type() + ":" + p.mode();
      if (!l.contains(s)) {
        l.add(s);
      }
    }
    return m;
  }
}
