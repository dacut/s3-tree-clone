TAG_VERSION := $(shell git tag --points-at)
COMMIT_VERSION := $(shell git rev-parse --short=10 HEAD)
IS_MODIFIED := $(shell git status --short --porcelain)
USERNAME := $(if $(LOGNAME),$(LOGNAME),$(if $(USER),$(USER),$(shell whoami)))
VERSION := $(if $(IS_MODIFIED),$(COMMIT_VERSION)-$(USERNAME),$(if $(TAG_VERSION),$(TAG_VERSION),$(COMMIT_VERSION)))

PLATFORMS := linux-amd64 linux-arm64 darwin-amd64 darwin-arm64
ZIP_TARGETS := $(foreach platform,$(PLATFORMS),s3-tree-clone-$(platform)-$(VERSION).zip)
EXE_TARGETS := $(foreach platform,$(PLATFORMS),s3-tree-clone-$(platform))

all: $(ZIP_TARGETS)

s3-tree-clone-%-$(VERSION).zip: go.mod go.sum *.go
	./build $@

clean:
	rm -rf s3-tree-clone-* s3-tree-clone tmp-*

.PHONY: all clean