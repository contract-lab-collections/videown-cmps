package shim

import (
	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
)

func FirstFilename(t *cesspat.FileMetadata) string {
	if t.Owner == nil || len(t.Owner) == 0 {
		return ""
	}
	return string(t.Owner[0].FileName)
}
