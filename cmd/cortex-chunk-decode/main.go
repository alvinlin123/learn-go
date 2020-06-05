package main

import (
    "bytes"
    "encoding/binary"
    "io"
    "io/ioutil"
    "os"

    "github.com/golang/snappy"
)

func run() error {
    in, err := ioutil.ReadAll(os.Stdin)
    if err != nil {
        return err
    }

    printMetadata(in)

    // TODO: print the time series data too.

    return nil
}

func printMetadata(raw []byte) {
    metadataSize := binary.BigEndian.Uint32(raw[0:4])
    metadataBytes := raw[4:metadataSize]

    io.Copy(os.Stdout, snappy.NewReader(bytes.NewReader(metadataBytes)))
}

func main() {
    if err := run(); err != nil {
        os.Stderr.WriteString(err.Error() + "\n")
        os.Exit(1)
    }
}