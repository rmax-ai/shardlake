.PHONY: all build release test test-shell lint clean demo

BINARY := ./target/debug/shardlake
DATA_DIR := /tmp/shardlake-demo
FIXTURE := fixtures/sample_10.jsonl

all: build

build:
	cargo build

release:
	cargo build --release

test:
	$(MAKE) test-shell
	cargo test

test-shell:
	./tools/test_loop_scheduler.sh

lint:
	cargo fmt --check
	cargo clippy -- -D warnings

clean:
	cargo clean
	rm -rf $(DATA_DIR)

demo: build
	@echo "=== Shardlake demo ==="
	rm -rf $(DATA_DIR)
	$(BINARY) --storage $(DATA_DIR) ingest --input $(FIXTURE) --dataset-version ds-v1
	$(BINARY) --storage $(DATA_DIR) build-index \
		--dataset-version ds-v1 \
		--index-version idx-v1 \
		--num-shards 2 \
		--metric cosine \
		--nprobe 2
	$(BINARY) --storage $(DATA_DIR) publish --index-version idx-v1
	$(BINARY) --storage $(DATA_DIR) benchmark --k 5 --nprobe 2
	@echo "=== Demo complete ==="
