package tparquet_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"

	tparquet "github.com/aperiodic-io/tparquet/go"
)

func testdataDir() string {
	if dir := os.Getenv("TPARQUET_TESTDATA"); dir != "" {
		return dir
	}
	return "../testdata"
}

// fileManifest is testdata/files/manifest.json (testdata/README.md).
type fileManifest struct {
	Files []struct {
		Name          string  `json:"name"`
		File          string  `json:"file"`
		Rows          int64   `json:"rows"`
		RowGroups     int     `json:"row_groups"`
		PcodecVersion *string `json:"pcodec_version"`
		Columns       []struct {
			Path         string         `json:"path"`
			PhysicalType string         `json:"physical_type"`
			Codecs       []string       `json:"codecs"`
			Pages        map[string]int `json:"pages"`
			Modes        map[string]int `json:"modes"`
		} `json:"columns"`
	} `json:"files"`
}

// payloadSpy records the payload mode of every page arrow-go decodes.
type payloadSpy struct {
	compress.ErrorCodec
	modes []string
}

func (s *payloadSpy) DecodeWithError(dst, src []byte) ([]byte, error) {
	switch {
	case len(src) == 0:
		s.modes = append(s.modes, "EMPTY")
	case len(src) < 2 || int(src[1]) >= 4:
		s.modes = append(s.modes, "INVALID")
	default:
		s.modes = append(s.modes, []string{"RAW", "PCO", "DECIMAL", "ZSTD"}[src[1]])
	}
	return s.ErrorCodec.DecodeWithError(dst, src)
}

// TestRustFilesMatchTheManifest reads the files the Rust writer made (testdata/files) page by
// page with arrow-go's low-level reader and checks what the manifest says about each: rows,
// row groups, the footer value, and per column the physical type, codec, page counts by
// page type and payload modes.
func TestRustFilesMatchTheManifest(t *testing.T) {
	dir := testdataDir()
	raw, err := os.ReadFile(filepath.Join(dir, "files", "manifest.json"))
	if errors.Is(err, os.ErrNotExist) {
		t.Skipf("no manifest in %s", dir)
	}
	if err != nil {
		t.Fatal(err)
	}
	var m fileManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	real, _ := compress.GetCodec(tparquet.Compression)
	spy := &payloadSpy{ErrorCodec: real.(compress.ErrorCodec)}
	compress.RegisterCodec(tparquet.Compression, spy)
	t.Cleanup(func() { compress.RegisterCodec(tparquet.Compression, real) })

	for _, f := range m.Files {
		t.Run(f.Name, func(t *testing.T) {
			rdr, err := file.OpenParquetFile(filepath.Join(dir, f.File), false)
			if err != nil {
				t.Fatal(err)
			}
			defer rdr.Close()
			md := rdr.MetaData()
			if md.NumRows != f.Rows || md.NumRowGroups() != f.RowGroups {
				t.Fatalf("%d rows in %d row groups, manifest %d in %d", md.NumRows, md.NumRowGroups(), f.Rows, f.RowGroups)
			}
			v, ok := footerValue(md, tparquet.FooterKey)
			if (f.PcodecVersion == nil) == ok || (ok && v != *f.PcodecVersion) {
				t.Fatalf("footer value %q (%v), manifest %v", v, ok, f.PcodecVersion)
			}
			if len(f.Columns) != md.Schema.NumColumns() {
				t.Fatalf("%d columns, manifest %d", md.Schema.NumColumns(), len(f.Columns))
			}
			for c, col := range f.Columns {
				if got := md.Schema.Column(c).Path(); got != col.Path {
					t.Fatalf("column %d is %s, manifest %s", c, got, col.Path)
				}
				if got := md.Schema.Column(c).PhysicalType().String(); got != col.PhysicalType {
					t.Errorf("%s: %s, manifest %s", col.Path, got, col.PhysicalType)
				}
				pages, modes := map[string]int{}, map[string]int{}
				codecs := map[string]bool{}
				pageLevel := true
				for g := 0; g < md.NumRowGroups(); g++ {
					rg := rdr.RowGroup(g)
					cc, err := rg.MetaData().ColumnChunk(c)
					if err != nil {
						t.Fatal(err)
					}
					name := cc.Compression().String()
					if cc.Compression() == tparquet.Compression {
						name = "PCODEC"
					}
					codecs[name] = true
					if cc.DataPageOffset() == 0 {
						// The Rust writer gives a chunk with no data pages data_page_offset
						// 0, below its dictionary page; arrow-go cannot read such a chunk's
						// pages (GetDictionaryPage panics): the codec is all to check.
						pageLevel = false
						continue
					}
					pr, err := rg.GetColumnPageReader(c)
					if err != nil {
						t.Fatal(err)
					}
					spy.modes = nil
					for pr.Next() {
						switch p := pr.Page().(type) {
						case *file.DictionaryPage:
							pages["DICTIONARY_PAGE"]++
						case *file.DataPageV1:
							pages["DATA_PAGE"]++
						case *file.DataPageV2:
							pages["DATA_PAGE_V2"]++
							if !p.IsCompressed() && cc.Compression() == tparquet.Compression {
								modes["UNCOMPRESSED"]++
							}
						}
					}
					if err := pr.Err(); err != nil {
						t.Fatal(err)
					}
					if cc.Compression() == tparquet.Compression {
						for _, mode := range spy.modes {
							modes[mode]++
						}
					}
				}
				if len(codecs) != len(col.Codecs) || !codecs[col.Codecs[0]] {
					t.Errorf("%s: codecs %v, manifest %v", col.Path, codecs, col.Codecs)
				}
				if !pageLevel {
					continue
				}
				if fmt.Sprint(pages) != fmt.Sprint(col.Pages) {
					t.Errorf("%s: pages %v, manifest %v", col.Path, pages, col.Pages)
				}
				if fmt.Sprint(modes) != fmt.Sprint(col.Modes) {
					t.Errorf("%s: modes %v, manifest %v", col.Path, modes, col.Modes)
				}
			}
		})
	}
}
