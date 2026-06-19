# macOS-compatible sed in-place
ifeq ($(shell uname),Darwin)
    SED_INPLACE := sed -i ''
else
    SED_INPLACE := sed -i
endif

REGISTRY ?= ghcr.io/aenix-org/kubernetes-switchcloud
TAG = $(shell git describe --tags --exact-match 2>/dev/null || echo latest)
PUSH := 1
LOAD := 0
BUILDER ?=
PLATFORM ?=
BUILDX_EXTRA_ARGS ?=
SWITCHCLOUD_VERSION = $(patsubst v%,%,$(shell git describe --tags || echo "v0.0.0-dev"))

BUILDX_ARGS := --provenance=false --push=$(PUSH) --load=$(LOAD) \
  --label org.opencontainers.image.source=https://github.com/aenix-org/kubernetes-switchcloud \
  $(if $(strip $(BUILDER)),--builder=$(BUILDER)) \
  $(if $(strip $(PLATFORM)),--platform=$(PLATFORM)) \
  $(BUILDX_EXTRA_ARGS)

# Returns 'latest' if the git tag is not assigned, otherwise returns the provided value
define settag
$(if $(filter $(TAG),latest),latest,$(1))
endef
