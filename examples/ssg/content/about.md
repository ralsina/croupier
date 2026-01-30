# About This SSG

This static site generator demonstrates the **power of hierarchical tasks** in Croupier.

## The Master/Subtask Pattern

Traditional build systems require you to manually define a task for each file. With Croupier's master/subtask pattern:

1. **Master task** - Watches a folder and dynamically manages subtasks
2. **Subtasks** - Regular tasks created on-demand for each file

### Benefits

- **No manual task definitions** - Subtasks are created automatically
- **Dynamic scaling** - Handles 1 or 1000 files equally well
- **Automatic cleanup** - Deleted files remove their subtasks
- **Incremental builds** - Only processes changed files

## Architecture

```
content/
├── index.md      ──┐
├── about.md      ──┤
└── blog/         ──┤   Master Task watches
    └── post.md   ──┘   this folder
                      │
                      ├─> Creates subtask per file
                      │
                      ▼
output/
├── index.html    (from index.md)
├── about.html    (from about.md)
└── blog/
    └── post.html (from blog/post.md)
```

## Try It Out

1. Add a new markdown file to `content/`
2. Run `./ssg`
3. See the new HTML in `output/`

The master task will automatically detect the new file and create a subtask for it!
