# macOS-compatible sed in-place
ifeq ($(shell uname),Darwin)
    SED_INPLACE := sed -i ''
else
    SED_INPLACE := sed -i
endif

REGISTRY ?= ghcr.io/aenix-org/kubernetes-switchcloud

# CACHE_REGISTRY: registry that `--cache-from` reads build cache from. It must
# point at a registry where a `:latest` tag is actually published.
CACHE_REGISTRY ?= ghcr.io/aenix-org/kubernetes-switchcloud

# IMAGE_TAG: build-unique tag pushed for every image. Set by CI to a value
# that does not collide between concurrent builds (e.g. a PR/sha tag); local
# builds default to `dev`.
IMAGE_TAG ?= dev

# Opt-in extra tags. Workflows set these explicitly; defaults are off so a
# local `make image` never races with CI or accidentally moves :latest.
#   PUBLISH_VERSIONED=1 -> also push :<component-version> (release semantics)
#   PUBLISH_FLOATING=1  -> also push :latest             (release/main only)
PUBLISH_VERSIONED ?= 0
PUBLISH_FLOATING  ?= 0

PUSH := 1
LOAD := 0
BUILDER ?=
PLATFORM ?=
BUILDX_EXTRA_ARGS ?=
COZYSTACK_VERSION = $(patsubst v%,%,$(shell git describe --tags --match 'v*'))

BUILDX_ARGS := --provenance=false --push=$(PUSH) --load=$(LOAD) \
  --label org.opencontainers.image.source=https://github.com/aenix-org/kubernetes-switchcloud \
  $(if $(strip $(BUILDER)),--builder=$(BUILDER)) \
  $(if $(strip $(PLATFORM)),--platform=$(PLATFORM)) \
  $(BUILDX_EXTRA_ARGS)

# image-tags <repo> <versioned-tag>
# Expands to one or more `--tag` flags for `docker buildx build`:
#   - always:                 :$(IMAGE_TAG)        (build-unique handle)
#   - if PUBLISH_VERSIONED=1: :<versioned-tag>     (skipped when arg2 is empty
#                                                  or equals IMAGE_TAG)
#   - if PUBLISH_FLOATING=1:  :latest
define image-tags
--tag $(REGISTRY)/$(1):$(IMAGE_TAG)$(if $(filter 1,$(PUBLISH_VERSIONED)),$(if $(filter-out $(IMAGE_TAG),$(strip $(2))), --tag $(REGISTRY)/$(1):$(strip $(2))))$(if $(filter 1,$(PUBLISH_FLOATING)), --tag $(REGISTRY)/$(1):latest)
endef

ifeq ($(COZYSTACK_VERSION),)
    $(shell git fetch --tags >/dev/null 2>&1 || true)
    COZYSTACK_VERSION = $(patsubst v%,%,$(shell git describe --tags --match 'v*'))
endif
