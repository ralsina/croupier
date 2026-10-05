#!/usr/bin/env crystal

# This example demonstrates a static site generator built on Croupier:
# one task per markdown file, where each render task depends on its
# source file, so only changed pages rebuild.
#
# Usage:
#   ./ssg              # Build once
#   ./ssg --auto        # Watch for changes and rebuild automatically
#   ./ssg -a           # Same as --auto

require "croupier"
require "markd"
require "file_utils"
require "option_parser"

# HTML template with PicoCSS styling
HTML_TEMPLATE = <<-HTML
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{title}}</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@1/css/pico.min.css">
  <style>
    body { padding-top: 2rem; }
    h1 { color: #e63946; }
    h2 { color: #457b9d; }
    code { background: #f1faee; padding: 0.2rem 0.4rem; border-radius: 4px; }
    pre { background: #1d3557; color: #f1faee; padding: 1rem; border-radius: 8px; overflow-x: auto; }
    pre code { background: transparent; color: inherit; }
    blockquote { border-left: 4px solid #e63946; padding-left: 1rem; color: #666; }
  </style>
</head>
<body>
  <main class="container">
    <nav>
      <ul>
        <li><strong><a href="index.html">SSG Example</a></strong></li>
        <li><a href="about.html">About</a></li>
        <li><a href="blog/first-post.html">Blog</a></li>
      </ul>
    </nav>
    <hr>
    <article>
      {{content}}
    </article>
    <footer>
      <p><small>Generated with Croupier - #{Time.local.to_s("%Y-%m-%d %H:%M")}</small></p>
    </footer>
  </main>
</body>
</html>
HTML

# Extract title from markdown content (first # header or default)
def extract_title(markdown : String, filename : String) : String
  lines = markdown.lines
  lines.each do |line|
    if match = line.match(/^#\s+(.+)$/)
      return match[1]
    end
  end
  File.basename(filename, ".md").capitalize
end

# Render markdown to HTML with template
def render_markdown(content : String, filename : String) : String
  options = Markd::Options.new(time: false, gfm: true)
  markdown_html = Markd.to_html(content, options)

  title = extract_title(content, filename)

  HTML_TEMPLATE
    .gsub("{{title}}", title)
    .gsub("{{content}}", markdown_html)
end

# Parse command line options
auto_mode = false

OptionParser.parse do |parser|
  parser.banner = "Usage: ssg [options]"
  parser.on("-a", "--auto", "Watch for changes and rebuild automatically") { auto_mode = true }
  parser.on("-h", "--help", "Show this help") { puts parser; exit }
  parser.on("-v", "--version", "Show version") { puts "SSG v0.1.0"; exit }
  parser.invalid_option do |flag|
    STDERR.puts "ERROR: #{flag} is not a valid option."
    STDERR.puts parser
    exit 1
  end
end

# Ensure directories exist
FileUtils.mkdir_p("content")
FileUtils.mkdir_p("output")
FileUtils.mkdir_p("output/blog")

# One render task per markdown file, created up front: each render
# depends on its source file, so a rebuild only re-renders pages
# whose content changed.
current_files = Dir.glob("content/**/*.md").to_set
current_files.each do |md_file|
  output_file = md_file.sub("content", "output").sub(".md", ".html")

  # Ensure output directory exists
  FileUtils.mkdir_p(File.dirname(output_file))

  Croupier::Task.new(
    inputs: [md_file],
    outputs: [output_file],
  ) do
    puts "  📝 Rendering #{md_file} -> #{output_file}"
    render_markdown(File.read(md_file), md_file)
  end
end

# Remove outputs whose source was deleted (nothing tracks deleted
# sources anymore, so compare the output tree against the sources)
expected_outputs = current_files.map do |md_file|
  md_file.sub("content", "output").sub(".md", ".html")
end.to_set
Dir.glob("output/**/*.html").each do |html|
  File.delete?(html) unless expected_outputs.includes?(html)
end

if auto_mode
  puts "=" * 60
  puts "🚀 Croupier SSG - Auto Mode"
  puts "=" * 60
  puts ""
  puts "👀 Watching markdown files for changes..."
  puts "   Press Ctrl+C to stop"
  puts ""

  # Set up a progress callback to show when files are rebuilt
  Croupier::TaskManager.progress_callback = ->(task_id : String) do
    puts "  ✓ Task completed: #{task_id}"
  end

  # Initial build
  Croupier::TaskManager.run_tasks

  puts ""
  puts "✅ Initial build complete!"
  puts ""
  puts "👀 Watching for changes... (Ctrl+C to stop)"
  puts ""
  puts "Auto mode detects:"
  puts "  - Modified files in content/ (rebuilds just those pages)"
  puts ""
  puts "Note: the task set is built at startup, so files added or"
  puts "deleted while watching need a restart (stop, run again)."
  puts ""

  # Start auto mode - this will watch for changes and rebuild
  Croupier::TaskManager.auto_run

  # Wait for the auto_run fiber to complete (runs until Ctrl+C)
  sleep
else
  puts "=" * 60
  puts "🚀 Croupier SSG Example"
  puts "=" * 60
  puts ""
  puts "📂 Building site from content/ folder..."
  puts ""

  # Run all render tasks; unchanged pages skip via early cutoff
  Croupier::TaskManager.run_tasks

  puts ""
  puts "=" * 60
  puts "✅ Build complete!"
  puts "=" * 60
  puts ""
  puts "📄 Generated files:"
  Dir.glob("output/**/*.html").sort.each do |f|
    size = File.info(f).size
    puts "  - #{f} (#{size} bytes)"
  end
  puts ""
  puts "💡 Tips:"
  puts "  - Edit files in content/ and run './ssg' again"
  puts "  - Run './ssg --auto' to watch for changes and rebuild automatically"
  puts "  - In auto mode, new/modified/deleted files are detected automatically"
  puts "  - Add new .md files and they'll be automatically included on next build"
  puts ""
end
