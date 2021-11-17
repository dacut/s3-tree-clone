TAG_VERSION := $(shell git tag --points-at)
COMMIT_VERSION := $(shell git rev-parse --short=10 HEAD)
IS_MODIFIED := $(shell git status --short --porcelain)
USERNAME := $(if $(LOGNAME),$(LOGNAME),$(if $(USER),$(USER),$(shell whoami)))
VERSION := $(if $(IS_MODIFIED),$(COMMIT_VERSION)-$(USERNAME),$(if $(TAG_VERSION),$(TAG_VERSION),$(COMMIT_VERSION)))
ARTIFACTORY_REPOSITORY = $(if $(IS_MODIFIED),general-develop,$(if $(TAG_VERSION),general,general-stage))
PLATFORMS := linux-x86_64 linux-aarch64 darwin-x86_64 darwin-aarch64
ZIP_TARGETS := $(foreach platform,$(PLATFORMS),s3-tree-clone-$(platform)-$(VERSION).zip)
EXE_TARGETS := $(foreach platform,$(PLATFORMS),s3-tree-clone-$(platform))
UPLOAD_TARGETS := $(foreach platform,$(PLATFORMS),upload-$(platform))

all: $(ZIP_TARGETS)

upload: $(UPLOAD_TARGETS)

upload-%: s3-tree-clone-%-$(VERSION).zip
	./artifactory-upload $(ARTIFACTORY_REPOSITORY) $<

s3-tree-clone-%-$(VERSION).zip: go.mod go.sum *.go
	./build $@

clean:
	rm -rf s3-tree-clone-* s3-tree-clone tmp-*

.PHONY: all clean