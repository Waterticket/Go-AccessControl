package main

import (
	"reflect"
	"unsafe"
)

// bit to string (unsafe)
func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// string to bit (unsafe)
func s2b(s string) (b []byte) {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Cap = sh.Len
	bh.Len = sh.Len
	return b
}
