// Reads a Parquet file (PCODEC or not) and writes it back with PCODEC (Auto policy).
//   downstream IN OUT
#include <iostream>

#include <tparquet/tparquet.h>

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: downstream IN OUT\n";
    return 2;
  }
  auto table = tparquet::ReadTable(argv[1]);
  if (!table.ok()) {
    std::cerr << table.status().ToString() << "\n";
    return 1;
  }
  tparquet::WriteOptions options;  // Auto, base ZSTD(3)
  if (auto st = tparquet::WriteTable(**table, argv[2], options); !st.ok()) {
    std::cerr << st.ToString() << "\n";
    return 1;
  }
  auto back = tparquet::ReadTable(argv[2]);
  if (!back.ok() || !(*back)->Equals(**table)) {
    std::cerr << "the copy differs\n";
    return 1;
  }
  std::cout << (*table)->num_rows() << " rows copied to " << argv[2] << "\n";
  return 0;
}
