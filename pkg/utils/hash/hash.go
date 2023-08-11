package hash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

const HashSize = sha256.Size

type H256 [HashSize]byte

func (t H256) Hex() string {
	return hex.EncodeToString(t[:])
}

func (t H256) String() string {
	return t.Hex()
}

func (t H256) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf(`"%s"`, t.Hex())), nil
}

func (t *H256) UnmarshalJSON(strBytes []byte) error {
	n := len(strBytes)
	if strBytes[0] != '"' || strBytes[n-1] != '"' {
		return errors.New("invalid json string value")
	}
	strBytes = strBytes[1 : n-1]
	n -= 2
	dn := HashSize * 2
	if n != dn && n != dn+2 {
		return errors.New("invalid sha256 hash hex string length, either 64 or 66 (with 0x prefix)")
	}
	toStrFn := func() string {
		if n == dn+2 {
			return string(strBytes[2:])
		}
		return string(strBytes)
	}
	b, err := hex.DecodeString(toStrFn())
	if err != nil {
		return err
	}
	copy(t[:], b)
	return nil
}

func Calculate(input io.Reader) (*H256, error) {
	h := sha256.New()
	if _, err := io.Copy(h, input); err != nil {
		return nil, err
	}
	return (*H256)(h.Sum(nil)), nil
}

func CalculateByBytes(data []byte) (*H256, error) {
	h := sha256.New()
	if _, err := h.Write(data); err != nil {
		return nil, err
	}
	return (*H256)(h.Sum(nil)), nil
}

func ValueOf(hexStr string) (*H256, error) {
	sb := []byte(hexStr)
	if sb[0] == '0' && (sb[1] == 'x' || sb[1] == 'X') {
		sb = sb[2:]
	}
	fh := H256{}
	_, err := hex.Decode(fh[:], sb)
	if err != nil {
		return nil, err
	}
	return &fh, nil
}
