# Welcome to SSG

This is a **static site generator** built with *Croupier* and the master/subtask pattern.

## Features

- Dynamic task creation
- Automatic rebuilds
- Real markdown support with `markd`
- Simple and extensible architecture

## How It Works

The `master_subtask.cr` file defines a **master task** that:

1. Watches the `content/` folder for changes
2. Automatically creates a **subtask** for each markdown file
3. Each subtask renders its markdown file to HTML

When you add, modify, or delete markdown files, the master task automatically updates the subtasks accordingly.

## Example Code

Here's some Crystal code:

```crystal
puts "Hello from Croupier SSG!"
```

And a list:

- First item
- Second item
- Third item

> This is a block quote demonstrating markdown rendering capabilities.

---

Try editing the markdown files in `content/` and running the SSG again to see the changes!
