# gocouchstore

Go bindings for Couchstore

## Building

1.  Obtain and build couchstore: https://github.com/couchbase/couchstore (run `make install` to install the library)
1.  Install header files to system location
  1. On Ubuntu 14.04: `cd <couchbase_build_dir> && mkdir /usr/local/include/libcouchstore && cp include/libcouchstore/* /usr/local/include/libcouchstore`
1.  `go get -u -v -t github.com/hisundar/gocouchstore`

## Documentation

See [godocs](http://godoc.org/github.com/hisundar/gocouchstore)
