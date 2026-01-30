#!/usr/bin/env crystal

# This example demonstrates hierarchical (master/subtask) tasks for a static site generator.
# A master task watches the `content/` folder and creates a subtask for each markdown file.
# When markdown files are added or removed, the subtasks are dynamically updated.
#
# Usage:
#   ./ssg              # Build once
#   ./ssg --auto        # Watch for changes and rebuild automatically
#   ./ssg -a           # Same as --auto

require "croupier"
require "markd"
require "digest"
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

# Master task watches content/ folder and creates subtask per markdown file
master_task = Croupier::Task.new(
  id: "content_master",
  inputs: ["content/"],
  always_run: true,
  master_task: true,
) do
  current_files = Dir.glob("content/**/*.md").to_set

  # Get previously created subtasks from k/v store
  previous_data = Croupier::TaskManager.get("content_subtasks")
  previous_files = previous_data ? previous_data.split("\n").to_set : Set(String).new

  # Remove subtasks for deleted files
  (previous_files - current_files).each do |deleted_file|
    puts "🗑️  Removing subtask for deleted file: #{deleted_file}"
    subtask_id = "render_#{Digest::SHA1.hexdigest(deleted_file)[0..6]}"
    Croupier::TaskManager.tasks.each do |key, task|
      Croupier::TaskManager.tasks.delete(key) if task.id == subtask_id
    end

    # Also remove output file
    output_path = deleted_file.sub("content", "output").sub(".md", ".html")
    File.delete?(output_path)
  end

  # Create subtasks for new/changed files
  (current_files - previous_files).each do |new_file|
    puts "✨ Creating subtask for new file: #{new_file}"
    subtask_id = "render_#{Digest::SHA1.hexdigest(new_file)[0..6]}"
    output_file = new_file.sub("content", "output").sub(".md", ".html")

    # Ensure output directory exists
    output_dir = File.dirname(output_file)
    FileUtils.mkdir_p(output_dir)

    subtask = Croupier::Task.new(
      id: subtask_id,
      inputs: [new_file],
      outputs: [output_file],
    ) do
      puts "  📝 Rendering #{new_file} -> #{output_file}"
      render_markdown(File.read(new_file), new_file)
    end

    Croupier::TaskManager.register_subtask("content_master", subtask)
  end

  # Save current list for next run
  Croupier::TaskManager.set("content_subtasks", current_files.to_a.join("\n"))

  nil  # Master tasks return nil
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

  # Build once initially to create subtasks
  Croupier::TaskManager.run_tasks
  Croupier::TaskManager.run_tasks

  puts ""
  puts "✅ Initial build complete!"
  puts ""
  puts "👀 Watching for changes... (Ctrl+C to stop)"
  puts ""
  puts "Auto mode detects:"
  puts "  - New files added to content/"
  puts "  - Modified files"
  puts "  - Deleted or moved files"
  puts ""

  # Start auto mode - this will watch for changes and rebuild
  Croupier::TaskManager.auto_run

  # Wait for the auto_run fiber to complete (runs until Ctrl+C)
  sleep
else
  puts "=" * 60
  puts "🚀 Croupier SSG - Master/Subtask Example"
  puts "=" * 60
  puts ""
  puts "📂 Building site from content/ folder..."
  puts ""

  # Run all tasks (master task will run first and create subtasks)
  Croupier::TaskManager.run_tasks

  # Run again to execute the newly created subtasks
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
