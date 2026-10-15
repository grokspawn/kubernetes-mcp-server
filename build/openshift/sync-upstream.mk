##@ Upstream Sync

UPSTREAM_REPO ?= containers/kubernetes-mcp-server
UPSTREAM_REMOTE ?= upstream
ORIGIN_REMOTE ?= origin
SYNC_BRANCH_NAME ?= upstream-sync
OWNER_REPO ?= $(shell git remote get-url $(ORIGIN_REMOTE) | sed 's|https://[^@]*@github\.com/||; s|https://github\.com/||; s|git@github\.com:||; s|\.git$$||')

UPSTREAM_SYNC_ARGS = --repo-dir "$(CURDIR)" \
	--upstream-remote "$(UPSTREAM_REMOTE)" \
	--upstream-repo "$(UPSTREAM_REPO)" \
	--downstream-remote "$(ORIGIN_REMOTE)" \
	--downstream-repo "$(OWNER_REPO)" \
	--owner-repo "$(OWNER_REPO)" \
	--sync-branch "$(SYNC_BRANCH_NAME)"

.PHONY: sync-upstream-check
sync-upstream-check: ## Inspect upstream sync plan without publishing
	@go run ./cmd/upstream-sync --mode=plan $(UPSTREAM_SYNC_ARGS)

.PHONY: sync-upstream-pr
sync-upstream-pr: ## Create or update the upstream sync PR (requires gh CLI)
	@go run ./cmd/upstream-sync --mode=publish $(UPSTREAM_SYNC_ARGS)
