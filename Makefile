all: test coverage

test:
	shards install
	crystal spec -v --error-trace
coverage: coverage/index.html
mutation: bin/crytic
	bin/crytic test
coverage/index.html: bin/run_tests
	rm -rf coverage/
	kcov --clean --include-path=./src $(PWD)/coverage ./bin/run_tests
	xdg-open coverage/index.html
bin/run_tests: src/*.cr spec/*.cr
	shards install
	crystal build -o bin/run_tests src/run_tests.cr
bin/crytic:
	shards install
lint:
	crystal tool format src/*.cr spec/*.cr
	bin/ameba --all --fix
clean:
	rm -rf lib/ bin/ coverage/ shard.lock
	git clean -f

.PHONY: clean coverage mutation test all
