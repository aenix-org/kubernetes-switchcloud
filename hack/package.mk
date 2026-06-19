.DEFAULT_GOAL := help
.PHONY: help show diff apply delete update image

# Run every recipe through bash with -e -o pipefail so a failing
# curl | tar pipeline (in package update targets) actually breaks the
# chain instead of being masked by tar's exit code on empty input.
SHELL := bash
.SHELLFLAGS := -eo pipefail -c

help: ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {sub("\\\\n",sprintf("\n%22c"," "), $$2);printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

show: check ## Show output of rendered templates
	cozyhr show --namespace $(NAMESPACE) $(NAME)

apply: check suspend ## Apply Helm release to a Kubernetes cluster
	cozyhr apply --namespace $(NAMESPACE) $(NAME)

diff: check ## Diff Helm release against objects in a Kubernetes cluster
	cozyhr diff --namespace $(NAMESPACE) $(NAME)

suspend: check ## Suspend reconciliation for an existing Helm release
	cozyhr suspend --namespace $(NAMESPACE) $(NAME)

resume: check ## Resume reconciliation for an existing Helm release
	cozyhr resume --namespace $(NAMESPACE) $(NAME)

delete: check suspend ## Delete Helm release from a Kubernetes cluster
	cozyhr delete --namespace $(NAMESPACE) $(NAME)

check:
	@if [ -z "$(NAME)" ]; then echo "env NAME is not set!" >&2; exit 1; fi
	@if [ -z "$(NAMESPACE)" ]; then echo "env NAMESPACE is not set!" >&2; exit 1; fi

clean:
	rm -rf charts/

%-update:
	helm repo add $(REPO_NAME) $(REPO_URL)
	helm repo update $(REPO_NAME)
	helm pull $(REPO_NAME)/$(CHART_NAME) --untar --untardir charts --version "$(CHART_VERSION)"
