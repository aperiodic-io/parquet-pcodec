package io.aperiodic.tparquet;

/** Run in a child JVM by {@link NativeLibraryProcessTest}: loads the library by its default route. */
public final class LoadProbe {
  private LoadProbe() {}

  public static void main(String[] args) throws Exception {
    byte[] src = PcodecTest.plain(PhysicalType.DOUBLE, 100);
    byte[] back = Pcodec.decompress(Pcodec.compress(src, PhysicalType.DOUBLE), src.length);
    System.out.println(java.util.Arrays.equals(src, back) ? "round trip ok" : "round trip differs");
  }
}
