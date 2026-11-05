package tparquet

// LinkedCodecID is the codec id the linked library reports.
func LinkedCodecID() int32 { return linkedCodecID() }

// LinkedCompressBound is the library's compress bound.
func LinkedCompressBound(n int64) int64 { return linkedCompressBound(n) }

// LinkedABIVersion is the ABI version of the linked library.
func LinkedABIVersion() uint32 { return linkedABIVersion() }
