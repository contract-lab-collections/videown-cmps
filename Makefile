GOFMT=gofmt
GC=go build

VERSION := $(shell git describe --always --tags --long)
BUILD_NODE_PAR = -ldflags "-X main.Version=$(VERSION)" #

ARCH=$(shell uname -m)
SRC_FILES = $(shell git ls-files | grep -e .go$ | grep -v _test.go)

cmps: $(SRC_FILES)
	$(GC)  $(BUILD_NODE_PAR) -o cmps main.go

cmps-cross: cmps-windows cmps-linux cmps-darwin

cmps-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GC) $(BUILD_NODE_PAR) -o cmps-windows-amd64.exe main.go

cmps-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GC) $(BUILD_NODE_PAR) -o cmps-linux-amd64 main.go

cmps-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GC) $(BUILD_NODE_PAR) -o cmps-darwin-amd64 main.go

tools-cross: tools-windows tools-linux tools-darwin

format:
	$(GOFMT) -w main.go

clean:
	rm -rf *.8 *.o *.out *.6 *exe
	rm -rf cmps cmps-*
