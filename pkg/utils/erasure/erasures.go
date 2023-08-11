package erasure

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/klauspost/reedsolomon"
	"github.com/pkg/errors"
)

var log logr.Logger

func init() {
	log = funcr.New(func(prefix, args string) {
		fmt.Println(prefix, args)
	}, funcr.Options{
		Verbosity: 3,
		LogCaller: funcr.All,
	}).WithName("erasure")
}

func SetLogger(logger logr.Logger) {
	log = logger.WithName("erasure")
}

type ShardCreateFunc func(index int) (io.WriteCloser, error)
type ShardOpenFunc func(index int) (r io.ReadCloser, size int64, err error)

type builderCommons struct {
	dataShards      int
	parityShards    int
	shardCreateFunc ShardCreateFunc
	shardOpenFunc   ShardOpenFunc
}

func (t *builderCommons) WithShards(dataShards int, parityShards int) *builderCommons {
	t.dataShards = dataShards
	t.parityShards = parityShards
	return t
}

func (t *builderCommons) WithShardCreater(shardCreateFunc ShardCreateFunc) *builderCommons {
	t.shardCreateFunc = shardCreateFunc
	return t
}

func (t *builderCommons) WithShardOpener(shardOpenFunc ShardOpenFunc) *builderCommons {
	t.shardOpenFunc = shardOpenFunc
	return t
}

func (t *builderCommons) validateArgs() error {
	if t.dataShards <= 0 {
		return errors.New("dataShards value invalid")
	}
	if t.parityShards <= 0 {
		return errors.New("dataShards value invalid")
	}
	if t.shardCreateFunc == nil {
		return errors.New("outputCreateFunc argument must not be nil")
	}
	if t.shardOpenFunc == nil {
		return errors.New("outputOpenFunc argument must not be nil")
	}
	return nil
}

type encoderBuilderCommons struct {
	builderCommons
	inputSize int64
}

func (t *encoderBuilderCommons) validateArgs() error {
	if err := t.builderCommons.validateArgs(); err != nil {
		return err
	}
	if t.inputSize <= 0 {
		return errors.New("inputSize value invalid")
	}
	return nil
}

type streamEncoderBuilder struct {
	encoderBuilderCommons
	inputStream io.ReadCloser
}

func RsecEncodeByStream(inputStream io.ReadCloser, inputSize int64) *streamEncoderBuilder {
	return &streamEncoderBuilder{
		encoderBuilderCommons{inputSize: inputSize},
		inputStream,
	}
}

func (t *streamEncoderBuilder) Build() (*RsecEncoder, error) {
	if t.inputStream == nil {
		return nil, errors.New("input argument must not be nil")
	}
	if t.dataShards <= 0 || t.parityShards <= 0 {
		t.dataShards, t.parityShards = reedSolomonRule(t.inputSize)
	}
	if err := t.validateArgs(); err != nil {
		return nil, err
	}
	return &RsecEncoder{
		t.inputStream,
		t.inputSize,
		t.dataShards,
		t.parityShards,
		t.shardCreateFunc,
		t.shardOpenFunc,
	}, nil
}

type fileEncoderBuilder struct {
	encoderBuilderCommons
	inputFile *os.File
}

func RsecEncodeByFile(inputFile *os.File) fileEncoderBuilder {
	return fileEncoderBuilder{
		inputFile: inputFile,
	}
}

func (t *fileEncoderBuilder) Build() (*RsecEncoder, error) {
	if t.inputFile == nil {
		return nil, errors.New("inputFile argument must not be nil")
	}
	fstat, err := t.inputFile.Stat()
	if err != nil {
		return nil, err
	}
	if t.inputSize = fstat.Size(); t.inputSize <= 0 {
		return nil, errors.Errorf("the inputFile: %s must not be empty", t.inputFile.Name())
	}
	if t.dataShards <= 0 || t.parityShards <= 0 {
		t.dataShards, t.parityShards = reedSolomonRule(t.inputSize)
	}
	if err := t.validateArgs(); err != nil {
		return nil, err
	}
	return &RsecEncoder{
		t.inputFile,
		t.inputSize,
		t.dataShards,
		t.parityShards,
		t.shardCreateFunc,
		t.shardOpenFunc,
	}, nil
}

type RsecEncoder struct {
	input            io.ReadCloser
	inputSize        int64
	dataShards       int
	parityShards     int
	outputCreateFunc ShardCreateFunc
	outputOpenFunc   ShardOpenFunc
}

func (t RsecEncoder) InputSize() int64  { return t.inputSize }
func (t RsecEncoder) DataShards() int   { return t.dataShards }
func (t RsecEncoder) ParityShards() int { return t.parityShards }

func (t *RsecEncoder) Encode() (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			err, _ := r.(error)
			retErr = err
			log.Error(err, "catch erasure encode panic", "inputSize", t.inputSize, "dataShards", t.dataShards, "parityShards", t.parityShards, "stack", string(debug.Stack()))
		}
	}()
	// Create encoding matrix.
	enc, err := reedsolomon.NewStream(t.dataShards, t.parityShards)
	if err != nil {
		return err
	}

	shards := t.dataShards + t.parityShards
	outputs := make([]io.WriteCloser, shards)
	for i := range outputs {
		outputs[i], err = t.outputCreateFunc(i)
		if err != nil {
			return errors.Wrapf(err, "create output for index:%d", i)
		}
	}

	// Split into writers.
	data := make([]io.Writer, t.dataShards)
	for i := range data {
		data[i] = outputs[i]
	}
	// Do the split
	err = enc.Split(t.input, data, t.inputSize)
	if err != nil {
		return errors.Wrap(err, "split")
	}

	// Close and re-open the outputs.
	inputDataShards := make([]io.Reader, t.dataShards)
	for i := range data {
		outputs[i].Close()
		rc, _, err := t.outputOpenFunc(i)
		if err != nil {
			return err
		}
		inputDataShards[i] = rc
		defer rc.Close()
	}

	// Create parity output writers
	parity := make([]io.Writer, t.parityShards)
	for i := range parity {
		parity[i] = outputs[t.dataShards+i]
		defer outputs[t.dataShards+i].Close()
	}

	// Encode parity
	return enc.Encode(inputDataShards, parity)
}

///////////////////////////////////////////////////////////////////////////////

type streamDecoderBuilder struct {
	builderCommons
	output           io.Writer
	expectOutputSize int64
}

func RsecDecodeByStream(output io.Writer, expectOutputSize int64) *streamDecoderBuilder {
	return &streamDecoderBuilder{
		output:           output,
		expectOutputSize: expectOutputSize,
	}
}

func (t *streamDecoderBuilder) Build() (*RsecDecoder, error) {
	if t.output == nil {
		return nil, errors.New("output argument must not be nil")
	}
	if t.dataShards <= 0 || t.parityShards <= 0 {
		t.dataShards, t.parityShards = reedSolomonRule(t.expectOutputSize)
	}
	if err := t.validateArgs(); err != nil {
		return nil, err
	}
	return &RsecDecoder{
		t.output,
		t.expectOutputSize,
		t.dataShards,
		t.parityShards,
		t.shardOpenFunc,
		t.shardCreateFunc,
	}, nil
}

type fileDecoderBuilder struct {
	streamDecoderBuilder
}

func RsecDecodeByFilePath(outputFilePath string, expectOutputSize int64) (*fileDecoderBuilder, error) {
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return nil, err
	}
	return &fileDecoderBuilder{
		streamDecoderBuilder{
			output:           outputFile,
			expectOutputSize: expectOutputSize,
		},
	}, nil
}

type RsecDecoder struct {
	output       io.Writer
	outputSize   int64
	dataShards   int
	parityShards int
	shardReader  ShardOpenFunc
	shardCreater ShardCreateFunc
}

func (t RsecDecoder) OutputSize() int64 { return t.outputSize }
func (t RsecDecoder) DataShards() int   { return t.dataShards }
func (t RsecDecoder) ParityShards() int { return t.parityShards }

// func NewRsecDecoder(output io.Writer, expectOutputSize int64, dataShards, parityShards int, shardReader ShardOpenFunc, shardCreater ShardCreateFunc) RsecDecoder {
// 	return RsecDecoder{
// 		output,
// 		expectOutputSize,
// 		dataShards,
// 		parityShards,
// 		shardReader,
// 		shardCreater,
// 	}
// }

func (t RsecDecoder) buildInputShardReaders() (r []io.Reader, size int64, err error) {
	// Create shards and load the data.
	shards := make([]io.Reader, t.dataShards+t.parityShards)
	for i := range shards {
		f, n, err := t.shardReader(i)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "fail read shard %d", i)
		}
		if n == 0 {
			log.Info("missing shard", "shardIndex", i)
			shards[i] = nil
			continue
		}
		shards[i] = f
		size = n
	}
	return shards, size, nil
}

func (t *RsecDecoder) Decode() (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			err, _ := r.(error)
			retErr = err
			log.Error(err, "catch erasure decode panic", "dataShards", t.dataShards, "parityShards", t.parityShards, "stack", string(debug.Stack()))
		}
	}()
	// Create matrix
	enc, err := reedsolomon.NewStream(t.dataShards, t.parityShards)
	if err != nil {
		return err
	}

	log.V(1).Info("begin erasure decode...")
	// Open the inputs
	shards, _, err := t.buildInputShardReaders()
	if err != nil {
		return err
	}

	// Verify the shards
	ok, err := enc.Verify(shards)
	if !ok {
		log.Info("Verification failed. Reconstructing data")
		shards, _, err = t.buildInputShardReaders()
		if err != nil {
			return err
		}
		// Create out destination writers
		out := make([]io.Writer, len(shards))
		for i := range out {
			if shards[i] == nil {
				log.V(1).Info("Creating shard", "shardIndex", i)
				out[i], err = t.shardCreater(i)
				if err != nil {
					return errors.Wrapf(err, "fail create shard %d", i)
				}
			}
		}
		err = enc.Reconstruct(shards, out)
		if err != nil {
			return errors.Wrap(err, "reconstruct failed")
		}
		// Close output.
		for i := range out {
			if out[i] != nil {
				err := out[i].(io.Closer).Close()
				if err != nil {
					return errors.Wrapf(err, "fail close shard %d", i)
				}
			}
		}
		shards, _, err = t.buildInputShardReaders()
		if err != nil {
			return err
		}
		ok, err = enc.Verify(shards)
		if err != nil {
			return errors.Wrap(err, "Verification failed after reconstruction")
		}
		if !ok {
			return errors.New("Verification failed after reconstruction, data likely corrupted")
		}
	}

	shards, _, err = t.buildInputShardReaders()
	// Join the shards and write them
	return enc.Join(t.output, shards, t.outputSize)
}

const SIZE_1MiB = 1024 * 1024

func reedSolomonRule(fsize int64) (int, int) {
	if fsize <= SIZE_1MiB*2560 {
		if fsize <= 1024 {
			return 1, 0
		}

		if fsize <= SIZE_1MiB*8 {
			return 2, 1
		}

		if fsize <= SIZE_1MiB*64 {
			return 4, 2
		}

		if fsize <= SIZE_1MiB*384 {
			return 6, 3
		}

		if fsize <= SIZE_1MiB*1024 {
			return 8, 4
		}

		return 10, 5
	}

	if fsize <= SIZE_1MiB*6144 {
		return 12, 6
	}

	if fsize <= SIZE_1MiB*7168 {
		return 14, 7
	}

	if fsize <= SIZE_1MiB*8192 {
		return 16, 8
	}

	if fsize <= SIZE_1MiB*9216 {
		return 18, 9
	}

	return 20, 10
}
