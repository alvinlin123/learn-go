package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/golang/snappy"
)

type datapoint struct {
	timestamp uint64
	value     float64
}

func run() error {
	in, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return err
	}

	printMetadata(in)
	fmt.Println()
	printTimeSeries(in)
	fmt.Println()

	return nil
}

func printTimeSeries(raw []byte) {
	metadataSize := binary.BigEndian.Uint32(raw[0:4])
	tsSize := binary.BigEndian.Uint32(raw[metadataSize : metadataSize+4])
	tsDataSlice := raw[metadataSize+4:]
	numLittleChunks := binary.LittleEndian.Uint16(tsDataSlice[0:2])

	fmt.Printf("Time series size is %v bytes\n", tsSize)
	fmt.Printf("Number of small chunks in big cuknsks: %v\n", numLittleChunks)

	printXorChunks(tsDataSlice[2:], numLittleChunks) //remove chunks count bytes
}

func printXorChunks(raw []byte, numChunks uint16) {

	chunkDataOffset := 0
	for i := 0; i < int(numChunks); i++ {
		chunkSize := binary.LittleEndian.Uint16(raw[chunkDataOffset : chunkDataOffset+2])

		fmt.Printf("chunk %v size is %v bytes\n", i, chunkSize)

		printXorChunk(raw[chunkDataOffset+2 : chunkDataOffset+2+int(chunkSize)])
		chunkDataOffset += 2 + int(chunkSize) //+2 for the 2 bytes that stores chunk size

		fmt.Println()
	}
}

func printXorChunk(raw []byte) {
	numSamplePoints := binary.BigEndian.Uint16(raw)
	fmt.Printf("\tnumber of sample points: %v\n", numSamplePoints)
}

func printMetadata(raw []byte) {
	metadataSize := binary.BigEndian.Uint32(raw[0:4])
	metadataBytes := raw[4:metadataSize]

	fmt.Printf("Metadata size: %v bytes\n", metadataSize)
	io.Copy(os.Stdout, snappy.NewReader(bytes.NewReader(metadataBytes)))
}

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}
