package wal

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"
)

func getSegmentPath(epoch uint32) string {
	return fmt.Sprintf("wal-%06d.log", epoch)
}

func naturalCmp(a, b string) int {
	// tokenizer solution for natural sort lol
	for len(a) > 0 && len(b) > 0 {
		aIsNum := a[0] >= '0' && a[0] <= '9'
		bIsNum := b[0] >= '0' && b[0] <= '9'

		if aIsNum != bIsNum {
			if aIsNum {
				return -1
			}
			return 1
		}

		if aIsNum {
			aNum, aRest := leadingInt(a)
			bNum, bRest := leadingInt(b)

			if aNum != bNum {
				return cmp.Compare(aNum, bNum)
			}

			// if equal, continue to the rest of string
			a, b = aRest, bRest
		} else {
			i, j := 0, 0
			for i < len(a) && !(a[i] >= '0' && a[i] <= '9') {
				i++
			}
			for j < len(b) && !(b[j] >= '0' && b[j] <= '9') {
				j++
			}
			if c := strings.Compare(a[:i], b[:j]); c != 0 {
				return c
			}
			a, b = a[i:], b[j:]
		}
	}
	// just return the shorter to longer strings (means most char are equal)
	return cmp.Compare(len(a), len(b))
}

func leadingInt(s string) (n int64, rest string) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	n, _ = strconv.ParseInt(s[:i], 10, 64)
	return n, s[i:]
}
