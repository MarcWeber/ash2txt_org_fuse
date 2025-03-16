module go_fs_project

go 1.23.0

toolchain go1.23.6

require (
	bazil.org/fuse v0.0.0-20230120002735-62a210ff1fd5
	github.com/PuerkitoBio/goquery v1.10.2
	github.com/hanwen/go-fuse/v2 v2.7.2
)

require (
	github.com/andybalholm/cascadia v1.3.3 // indirect
	golang.org/x/exp v0.0.0-20250305212735-054e65f0b394 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
)

replace bazil.org/fuse => github.com/bazil/fuse v0.0.0-20230120002735-62a210ff1fd5
