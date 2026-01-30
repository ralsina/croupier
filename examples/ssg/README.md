# Croupier SSG - Master/Subtask Example

A simple static site generator demonstrating Croupier's **hierarchical (master/subtask) tasks** feature.

## What This Demonstrates

This example shows how to use master tasks to dynamically create and manage subtasks:

- **Master Task** (`content_master`) - Watches the `content/` folder and automatically creates a subtask for each markdown file
- **Subtasks** - Regular tasks that render individual markdown files to HTML
- **Dynamic Scaling** - Add or remove markdown files, and the build system adapts automatically
- **Incremental Builds** - Only processes files that have changed

## Features

- ✨ Real markdown parsing with [markd](https://github.com/icyleaf/markd)
- 🎨 Beautiful HTML output with [PicoCSS](https://picocss.com/)
- 📁 Preserves directory structure (e.g., `content/blog/` → `output/blog/`)
- 🔄 Automatic subtask management
- ⚡ Incremental builds (only re-renders changed files)
- 👁️ **Auto mode** - Watch for changes and rebuild automatically, including detecting new files
- 🗑️ Cleanup of deleted files

## Installation

1. Install dependencies:
   ```bash
   shards install
   ```

2. Build the binary:
   ```bash
   shards build
   # or
   crystal build src/ssg.cr -o bin/ssg
   ```

## Usage

### Build once

```bash
./bin/ssg
```

### Auto mode (watch for changes)

```bash
./bin/ssg --auto
# or
./bin/ssg -a
```

In auto mode, the SSG will:
1. Build the site initially
2. Watch the `content/` folder for changes
3. Automatically rebuild when:
   - Files are created
   - Files are modified
   - Files are deleted or moved
4. Only reprocess the files that changed

Press `Ctrl+C` to stop watching.

### Command-line options

```
Usage: ssg [options]
    -a, --auto      Watch for changes and rebuild automatically
    -h, --help      Show this help
    -v, --version   Show version
```

### Output

The generated HTML files will be in the `output/` directory:

```
output/
├── index.html       (from content/index.md)
├── about.html       (from content/about.md)
└── blog/
    └── first-post.html  (from content/blog/first-post.md)
```

### Add new content

Just add a new `.md` file to the `content/` folder:

```bash
# Create a new blog post
cat > content/blog/new-post.md << 'EOF'
# My New Post

This is a new blog post!
EOF

# In auto mode, it's detected automatically!
# In normal mode, just run ./bin/ssg again
```

### Modify existing content

Edit any markdown file in `content/` and the SSG will automatically re-process only that file.

### Delete content

Delete a markdown file and the corresponding HTML file will be removed automatically.

## How It Works

### The Master Task

```crystal
master_task = Croupier::Task.new(
  id: "content_master",
  inputs: ["content/"],  # Watch the content folder
  always_run: true,
  master_task: true,     # This is a master task
) do
  # Scan content/ folder for markdown files
  current_files = Dir.glob("content/**/*.md").to_set

  # Compare with previous run (from k/v store)
  previous_files = ... # load from k/v store

  # Remove subtasks for deleted files
  (previous_files - current_files).each do |deleted_file|
    subtask_id = "render_#{Digest::SHA1.hexdigest(deleted_file)[0..6]}"
    # Remove the subtask and its output file
  end

  # Create subtasks for new files
  (current_files - previous_files).each do |new_file|
    subtask = Croupier::Task.new(
      id: "render_#{Digest::SHA1.hexdigest(new_file)[0..6]}",
      inputs: [new_file],
      outputs: [output_file],
    ) do
      render_markdown(File.read(new_file), new_file)
    end

    Croupier::TaskManager.register_subtask("content_master", subtask)
  end
end
```

### Subtasks

Each subtask is a regular Croupier task that:

1. Takes a markdown file as input
2. Renders it to HTML
3. Writes the output file

```crystal
subtask = Croupier::Task.new(
  id: "render_#{file_hash}",
  inputs: [markdown_file],
  outputs: [html_output_file],
) do
  render_markdown(File.read(markdown_file), markdown_file)
end
```

## Benefits of Master/Subtask Pattern

### Traditional Approach ❌

```crystal
# Must manually define a task for each file
Task.new(inputs: ["content/index.md"], outputs: ["output/index.html"]) { ... }
Task.new(inputs: ["content/about.md"], outputs: ["output/about.html"]) { ... }
Task.new(inputs: ["content/blog/post1.md"], outputs: ["output/blog/post1.html"]) { ... }
# ... and so on for every file
```

### Master/Subtask Approach ✅

```crystal
# One master task handles everything
Task.new(master_task: true) do
  # Automatically creates subtasks for each file
end
```

## Project Structure

```
ssg/
├── shard.yml           # Project dependencies
├── src/
│   └── ssg.cr          # Main application
├── README.md           # This file
├── content/            # Source markdown files
│   ├── index.md
│   ├── about.md
│   └── blog/
│       └── first-post.md
└── output/             # Generated HTML (created on build)
    ├── index.html
    ├── about.html
    └── blog/
        └── first-post.html
```

## Customization

### Change the HTML Template

Edit the `HTML_TEMPLATE` constant in `src/ssg.cr` to customize the generated HTML.

### Change Styling

The default uses PicoCSS via CDN. You can:
- Use a different CSS framework
- Add custom styles in the `<style>` tag
- Link to an external stylesheet

### Add Processing Steps

Modify the `render_markdown` function to add:
- Syntax highlighting for code blocks
- Image optimization
- Table of contents generation
- Custom markdown extensions

## License

MIT License - See main Croupier project for details.
