IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= dev
ANSWER ?= dev-answer
BIN   := target/release/shopping-sessions
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev local local-fixtures local-dev

## Docker image for the scorer's platform
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 fixtures and diff against expected/
fixtures: build
	python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

## 1M-event dev set (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run the image on dev/ with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Diff out/ against the published answer (dev-answer.zip unzipped to dev-answer/)
check-dev:
	python3 scripts/compare.py out $(ANSWER)

## Native loop without Docker (Linux x86-64 with AVX-512, or edit .cargo/config.toml)
local:
	cargo build --release

local-fixtures: local
	python3 scripts/run_fixtures.py fixtures -- $(BIN)

local-dev: local
	rm -rf out && time env DATA_DIR=$(DEV) OUTPUT_DIR=out $(BIN) && python3 scripts/compare.py out $(ANSWER)
