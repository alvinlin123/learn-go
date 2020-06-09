package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"time"
)

type xorChunkReader struct {
	b                     []byte
	bytePos               int64
	bitMask               byte
	prevDp                *datapoint
	dpRead                uint32
	prevLeadingZeroes     uint32
	prevNumMeaningfulBits uint32
	prevTimeDelta         int64
}

type datapoint struct {
	timestamp int64
	value     float64
}

func NewXorChunkReader(b []byte) *xorChunkReader {
	val := &xorChunkReader{b: b, bitMask: 0x80}

	return val
}

func (cr *xorChunkReader) NextDatapoint() *datapoint {
	var dp *datapoint

	if cr.dpRead == 0 {
		r := bytes.NewReader(cr.getSliceOfSize(binary.MaxVarintLen64))
		ts, _ := binary.ReadVarint(r)
		bytesRead := r.Size() - int64(r.Len())
		valBits := binary.BigEndian.Uint64(cr.b[bytesRead : bytesRead+8])
		val := math.Float64frombits(valBits)

		dp = &datapoint{timestamp: ts, value: val}
		cr.bytePos = bytesRead + 8
	} else if cr.dpRead == 1 {
		r := bytes.NewReader(cr.getSliceOfSize(binary.MaxVarintLen64))
		tDelta, _ := binary.ReadVarint(r)
		bytesRead := r.Size() - int64(r.Len())
		cr.bytePos += bytesRead
		cr.prevTimeDelta = tDelta
		val := cr.readValue()
		dp = &datapoint{timestamp: cr.prevDp.timestamp + tDelta, value: val}
	} else {
		dp = &datapoint{timestamp: cr.readTimeStamp(), value: cr.readValue()}
	}

	cr.dpRead++
	cr.prevDp = dp
	return dp
}

func (cr *xorChunkReader) readTimeStamp() int64 {
	tCtrlBit := cr.readBits(1)

	var dodBitSize uint8
	if tCtrlBit == 0 {
		dodBitSize = 0
	} else if tCtrlBit = cr.readBits(1); tCtrlBit == 0 {
		//10 case
		dodBitSize = 14
	} else if tCtrlBit = cr.readBits(1); tCtrlBit == 0 {
		//110 case
		dodBitSize = 17
	} else if tCtrlBit = cr.readBits(1); tCtrlBit == 0 {
		//1110 case
		dodBitSize = 20
	} else {
		//1111 case
		dodBitSize = 64
	}

	var dod uint64
	if dodBitSize > 0 {
		dod = cr.readBits(uint32(dodBitSize))

		if dodBitSize != 64 && dod > (1<<(dodBitSize-1)) {
			//Need to convert number to negative is sign bit is on, else whn cast to int64 it will
			//remain positive number.
			//We are using dodBitSize bits to represent number in range
			//[-(2^(dodBitSize - 1) -1), 2^(dodBitSize - 1)]
			// e.g. if dodBitSize is 14, the number range is [-(2^13 - 1), (2^13)]

			dod = dod - (1 << dodBitSize) //Flip the leading zeroes into 1s.
		}
	}

	return int64(dod) + cr.prevTimeDelta + cr.prevDp.timestamp
}

func (cr *xorChunkReader) readValue() float64 {
	zeroDeltaBitIndicator := cr.readBits(1)

	if zeroDeltaBitIndicator == 0 {
		return cr.prevDp.value
	}

	controlBit := cr.readBits(1)

	var floatInBits uint64
	if controlBit == 0 {
		floatInBits = cr.readBits(cr.prevNumMeaningfulBits)
		floatInBits <<= 64 - cr.prevLeadingZeroes - cr.prevLeadingZeroes
	} else {
		numLeading := uint32(cr.readBits(5))
		numMeaningful := uint32(cr.readBits(6))
		floatInBits := cr.readBits(numMeaningful)

		cr.prevLeadingZeroes = numLeading
		cr.prevNumMeaningfulBits = numMeaningful

		floatInBits <<= 64 - numLeading - numMeaningful
	}

	return math.Float64frombits(math.Float64bits(cr.prevDp.value) ^ floatInBits)
}

func (cr *xorChunkReader) readBits(numBits uint32) uint64 {
	var result uint64

	for i := uint32(0); i < numBits; i++ {
		bit8 := cr.b[cr.bytePos] & cr.bitMask
		bit8 >>= bits.TrailingZeros8(bit8)

		bit := uint64(bit8)
		result <<= 1
		result |= bit

		if cr.bitMask == 1 {
			cr.bitMask = 0x80
			cr.bytePos++
		} else {
			cr.bitMask >>= 1
		}
	}

	return result
}

func (cr *xorChunkReader) getSliceOfSize(s int64) []byte {
	return cr.b[cr.bytePos : cr.bytePos+s]
}

func (dp *datapoint) String() string {
	t := time.Unix(dp.timestamp/1000, (dp.timestamp%1000)*1000000)
	t = t.UTC()
	return fmt.Sprintf("[ts: %v, val: %v]", t.Format(time.RFC3339), dp.value)
}
