export NAME=kilo-clustermesh-operator
export NAMESPACE=cozy-$(NAME)

include ../../../hack/common-envs.mk
include ../../../hack/package.mk

# Version of the upstream generic operator chart published to
# oci://ghcr.io/cozystack/charts by the kilo-clustermesh-operator release.
CHART_VERSION ?= 0.6.0

update:
	rm -rf charts
	helm pull oci://ghcr.io/cozystack/charts/kilo-clustermesh-operator \
		--untar --untardir charts --version "$(CHART_VERSION)"
