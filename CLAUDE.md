# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code
in this repository.

## Project Overview

Croupier is a Crystal library for smart task definition and execution using
dataflow programming. It creates dependency graphs between tasks and only runs
what's necessary based on file/content changes.

## Build System and Development Commands

**Building:**

- `shards build` - Build the project

- `shards install` - Install dependencies

**Testing:**

- `crystal spec` - Run tests

- `make test` - Run tests with verbose output and error traces

- `crystal spec spec/croupier_spec.cr` - Run specific test file

**Linting and Formatting:**

- `crystal tool format src/*.cr spec/*.cr` - Format code

- `bin/ameba --all --fix` - Run linter with auto-fix

- `make lint` - Run both formatting and linting with fixes

**Coverage and Analysis:**

- `make coverage` - Generate test coverage report (opens in browser)

- `make mutation` - Run mutation testing

- `bin/crytic` - Run mutation testing manually

**Cleanup:**

- `make clean` - Remove build artifacts, lib/, coverage/, shard.lock

## Architecture

**Core Components:**

- **TaskManager** (`src/croupier.cr:21-540`): Singleton that manages the task graph,
  tracks file changes, handles execution modes (serial/parallel/auto), and maintains
  k/v store

- **Task** (`src/task.cr:12-283`): Represents individual tasks with inputs/outputs,
  procs, and execution logic. Supports task merging when outputs conflict

- **Topological Sort** (`src/topo_sort.cr`): Implements dependency resolution for
  task execution order

**Key Concepts:**

- Tasks have inputs (files, task outputs, or k/v store keys) and outputs (files
  or k/v store keys)

- Dependency graph is built automatically and sorted topologically

- Tasks only run when inputs change or outputs are missing

- Supports k/v store with `kv://key` syntax for intermediate data

- Auto mode uses inotify to watch files and trigger rebuilds

- Tasks can be merged if they share outputs (configurable via `mergeable` flag)

**Execution Modes:**

- `run_tasks` - Execute all stale tasks (serial or parallel)

- `auto_run` - Continuous mode watching for file changes

- Targeted execution - Run specific tasks and their dependencies

**State Management:**

- `.croupier` file stores SHA1 hashes of input files between runs

- Fast mode uses file timestamps instead of content hashes

- K/v store can be in-memory or persistent (file-based)

## Testing

Test files are in `spec/` directory with comprehensive coverage including:

- Basic task execution and dependency resolution

- Auto mode functionality with inotify

- Task merging behavior

- K/v store operations

- Parallel execution scenarios

Test cases use temporary directories and files for isolation.
