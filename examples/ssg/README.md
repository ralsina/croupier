# Croupier SSG Example

A simple static site generator demonstrating incremental builds: one
Croupier task per markdown file, each depending on its source.

## What This Demonstrates

- **One task per page** - Each markdown file gets a render task whose
  input is the source file, so only changed pages rebuild
- **Incremental Builds** - Only processes files that have changed
- **Directory structure** - `content/blog/` maps to `output/blog/`

## Features

- ✨ Real markdown parsing with [markd](https://github.com/icyleaf/markd)
- 🎨 Beautiful HTML output with [PicoCSS](https://picocss.com/)
- 📁 Preserves directory structure (e.g., `content/blog/` → `output/blog/`)
- ⚡ Incremental builds (only re-renders changed files)
- 👁️ **Auto mode** - Watch for changes and rebuild automatically, including detecting new files
- 🗑️ Cleanup of outputs for deleted sources

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

```text
Usage: ssg [options]
    -a, --auto      Watch for changes and rebuild automatically
    -h, --help      Show this help
    -v, --version   Show version
```

### Output

The generated HTML files will be in the `output/` directory:

```text
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

One render task per markdown file, created up front from the
`content/` tree:

```crystal
Dir.glob("content/**/*.md").each do |md_file|
  output_file = md_file.sub("content", "output").sub(".md", ".html")
  FileUtils.mkdir_p(File.dirname(output_file))

  Croupier::Task.new(
    inputs: [md_file],
    outputs: [output_file],
  ) do
    render_markdown(File.read(md_file), md_file)
  end
end

# Remove outputs whose source was deleted
expected_outputs = Dir.glob("content/**/*.md").map do |md_file|
  md_file.sub("content", "output").sub(".md", ".html")
end.to_set
Dir.glob("output/**/*.html").each do |html|
  File.delete?(html) unless expected_outputs.includes?(html)
end
```

Each render task:

1. Takes its markdown file as input
2. Renders it to HTML with the PicoCSS template
3. Writes the output file

Re-running does nothing until a source file changes — then only that
page's task re-runs, and downstream pages are untouched.

## Project Structure

```text
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
