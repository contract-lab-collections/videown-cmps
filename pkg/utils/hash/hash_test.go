package hash

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashToHex(t *testing.T) {
	fh, err := Calculate(strings.NewReader("Hello World!"))
	require.NoError(t, err)
	h := fh.Hex()
	t.Log(h)
	require.Equal(t, HashSize*2+2, len(h))
}

func TestAboutJson(t *testing.T) {
	require := require.New(t)
	fh, err := Calculate(strings.NewReader("Hello World!"))
	require.NoError(err)

	b, err := json.Marshal(fh)
	require.NoError(err)
	jsonValue := string(b)
	t.Log(jsonValue)
	require.Equal(`"0x7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"`, jsonValue)

	var fh1 H256
	err = json.Unmarshal(b, &fh1)
	require.NoError(err)
	require.Equal(*fh, fh1)
}

func TestNewByHex(t *testing.T) {
	fh, err := ValueOf("0x7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069")
	require.NoError(t, err)
	t.Log(fh)
	fh1, err := ValueOf("7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069")
	require.NoError(t, err)
	t.Log(fh1)
}
