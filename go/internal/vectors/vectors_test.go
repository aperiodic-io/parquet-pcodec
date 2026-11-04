package vectors

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"

	pparquet "github.com/aperiodic-io/pparquet/go"
)

func TestGenerateThenCheck(t *testing.T) {
	dir := t.TempDir()
	names, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) < 8 {
		t.Fatalf("only %d vectors: %v", len(names), names)
	}
	for _, name := range names {
		for _, p := range []string{filepath.Join(dir, "files", name+".parquet"), filepath.Join(dir, "expected", name+".jsonl")} {
			if _, err := os.Stat(p); err != nil {
				t.Fatal(err)
			}
		}
	}
	n, err := Check(dir)
	if err != nil || n != len(names) {
		t.Fatalf("Check: %d files, %v", n, err)
	}
}

func TestCheckReportsDifferences(t *testing.T) {
	dir := t.TempDir()
	names, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	// A changed value in one expected file.
	p := filepath.Join(dir, "expected", names[0]+".jsonl")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitN(string(b), "\n", 2)
	if err := os.WriteFile(p, []byte(lines[0]+"\n"+lines[0]+"\n"+lines[1]), 0o644); err != nil {
		t.Fatal(err)
	}
	// A missing expected file for another.
	if err := os.Remove(filepath.Join(dir, "expected", names[1]+".jsonl")); err != nil {
		t.Fatal(err)
	}
	// An unreadable Parquet file.
	if err := os.WriteFile(filepath.Join(dir, "files", "zz_broken.parquet"), []byte("PAR1 nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "expected", "zz_broken.jsonl"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	n, err := Check(dir)
	if n != len(names)+1 {
		t.Fatalf("checked %d files", n)
	}
	for _, want := range []string{names[0] + ": ", names[1] + ": ", "zz_broken: "} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want it to name %q", err, want)
		}
	}
	if strings.Contains(err.Error(), names[2]+": ") {
		t.Fatalf("a good vector was reported: %v", err)
	}
}

func TestCheckWithoutVectors(t *testing.T) {
	_, err := Check(filepath.Join(t.TempDir(), "absent"))
	if !errors.Is(err, ErrNoVectors) {
		t.Fatalf("err = %v", err)
	}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "files"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Check(dir); !errors.Is(err, ErrNoVectors) {
		t.Fatalf("an empty files directory: err = %v", err)
	}
}

func TestGenerateIsDeterministic(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	names, err := Generate(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Generate(b); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		for _, rel := range []string{filepath.Join("files", name+".parquet"), filepath.Join("expected", name+".jsonl")} {
			x, _ := os.ReadFile(filepath.Join(a, rel))
			y, _ := os.ReadFile(filepath.Join(b, rel))
			if string(x) != string(y) || len(x) == 0 && strings.HasSuffix(rel, ".parquet") {
				t.Fatalf("%s differs between runs", rel)
			}
		}
	}
}

func TestGenerateIntoAFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(f, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Generate(f); err == nil {
		t.Fatal("generating into a regular file should fail")
	}
}

func TestVectorsCoverThePageKinds(t *testing.T) {
	dir := t.TempDir()
	if _, err := Generate(dir); err != nil {
		t.Fatal(err)
	}
	open := func(name string) *file.Reader {
		r, err := file.OpenParquetFile(filepath.Join(dir, "files", name+".parquet"), false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		return r
	}
	codecs := func(r *file.Reader) (pco, other int, dict bool) {
		for i := 0; i < r.NumRowGroups(); i++ {
			md := r.MetaData().RowGroup(i)
			for j := 0; j < md.NumColumns(); j++ {
				cc, _ := md.ColumnChunk(j)
				if cc.Compression() == pparquet.Compression {
					pco++
					dict = dict || cc.HasDictionaryPage()
				} else {
					other++
				}
			}
		}
		return
	}
	for _, name := range []string{"go_ints", "go_float_decimals", "go_special_floats", "go_temporal", "go_nested",
		"go_v1_pages", "go_dictionary_pages", "go_fallback", "go_auto_mixed"} {
		r := open(name)
		if v := r.MetaData().KeyValueMetadata().FindValue(pparquet.FooterKey); v == nil || *v != "1" {
			t.Errorf("%s: no footer key", name)
		}
		pco, other, dict := codecs(r)
		if pco == 0 {
			t.Errorf("%s: no PCODEC column chunk", name)
		}
		if (name == "go_auto_mixed") != (other > 0) {
			t.Errorf("%s: %d base column chunks", name, other)
		}
		if (name == "go_dictionary_pages") != dict {
			t.Errorf("%s: PCODEC dictionary pages %v", name, dict)
		}
	}
	if n := open("go_auto_mixed").NumRowGroups(); n < 3 {
		t.Errorf("go_auto_mixed has %d row groups", n)
	}
	r := open("go_v1_pages")
	pr, err := r.RowGroup(0).GetColumnPageReader(0)
	if err != nil {
		t.Fatal(err)
	}
	pages := 0
	for pr.Next() {
		if pr.Page().Type() != file.PageTypeDataPage {
			t.Fatalf("page type %v", pr.Page().Type())
		}
		pages++
	}
	if pages < 2 {
		t.Fatalf("go_v1_pages has %d pages", pages)
	}
}
