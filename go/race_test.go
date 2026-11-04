package pcodec_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"

	pcodec "github.com/aperiodic-io/parquet-pcodec/go"
)

// TestConcurrentUse runs the codec and the writer from many goroutines at once; run it with
// -race. Decode errors are per call even though the library keeps its last error per OS
// thread: each goroutine checks it gets its own message.
func TestConcurrentUse(t *testing.T) {
	c, _ := compress.GetCodec(pcodec.Compression)
	ec := c.(compress.ErrorCodec)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 1))
			for i := 0; i < 200; i++ {
				col := genNumbers(r, r.IntN(2000))
				enc := c.EncodeLevel(nil, col.data, pcodec.Level(col.typ, r.IntN(13)))
				if !bytes.Equal(c.Decode(make([]byte, len(col.data)), enc), col.data) {
					t.Error("round trip failed")
					return
				}
				// A wrong size gives this call's error, not another goroutine's.
				size := len(col.data) + 8*(g+1)
				_, err := ec.DecodeWithError(make([]byte, size), enc)
				if err == nil || !strings.Contains(err.Error(), fmt.Sprint(size)) {
					t.Errorf("err = %v, want one naming the size %d", err, size)
					return
				}
			}
			rec := autoRecord(3000)
			defer rec.Release()
			var buf bytes.Buffer
			w, err := pcodec.NewFileWriter(rec.Schema(), &buf, pcodec.WithWriterProperties(parquet.WithMaxRowGroupLength(1000)))
			if err == nil {
				err = w.Write(rec)
			}
			if err == nil {
				err = w.Close()
			}
			if err != nil {
				t.Error(err)
				return
			}
			assertSameData(t, buf.Bytes(), rec)
		}(g)
	}
	wg.Wait()
}
