package download

const (
	minChunkSize = 1 << 20  // 1 MiB
	maxChunkSize = 16 << 20 // 16 MiB
	targetChunks = 64
)

// chunkSpec describes a half-open byte range [start, end).
type chunkSpec struct {
	index int
	start int64
	end   int64
}

func (c chunkSpec) size() int64 { return c.end - c.start }

// planChunks picks a chunk size that aims for targetChunks pieces, clamped to
// [1MiB, 16MiB].
func planChunks(total int64) int64 {
	if total <= 0 {
		return 0
	}
	size := total / targetChunks
	if size < minChunkSize {
		size = minChunkSize
	}
	if size > maxChunkSize {
		size = maxChunkSize
	}
	return size
}

func buildChunks(total, size int64) []chunkSpec {
	if total <= 0 || size <= 0 {
		return nil
	}
	count := int((total + size - 1) / size)
	chunks := make([]chunkSpec, 0, count)
	for i := 0; i < count; i++ {
		start := int64(i) * size
		end := start + size
		if end > total {
			end = total
		}
		chunks = append(chunks, chunkSpec{index: i, start: start, end: end})
	}
	return chunks
}

func countTrue(bits []bool) int {
	n := 0
	for _, b := range bits {
		if b {
			n++
		}
	}
	return n
}

// bytesDone returns the number of completed bytes represented by a chunk
// bitmap for a file of the given total size and fixed chunk size.
func bytesDone(done []bool, size, total int64) int64 {
	var n int64
	for i, ok := range done {
		if !ok {
			continue
		}
		chunkLen := size
		if int64(i+1)*size > total {
			chunkLen = total - int64(i)*size
		}
		if chunkLen > 0 {
			n += chunkLen
		}
	}
	return n
}
