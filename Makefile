

help: ## Display this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

build: ## Build the project
	cargo build --all-targets --all-features --workspace

check-fmt: ## Check the formatting of the code
	cargo  fmt --all -- --check

check-clippy: ## Check the clippy of the code
	cargo  clippy --all-targets --all-features --workspace -- -D warnings

check: check-fmt check-clippy

doc-test: ## Test the documentation of the code
	cargo test --no-fail-fast --doc --all-features --workspace

unit-test: doc-test ## Test the unit tests of the code
	cargo test --no-fail-fast --lib --all-features --workspace

test: doc-test ## Test the code
	cargo test --no-fail-fast --all-targets --all-features --workspace

clean: ## Clean the project
	cargo clean