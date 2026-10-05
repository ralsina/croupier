# Welcome to SSG

This is a **static site generator** built with *Croupier*: one task
per markdown file, each render depending on its source file.

## How It Works

At startup the generator creates one Croupier task per page:

1. Each task takes its markdown file as input
2. Renders it to HTML with the PicoCSS template
3. Writes the output file

Re-running does nothing until a source file changes — then only that
page's task re-runs.

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

Try editing the markdown files in `content/` and running the SSG
again to see the changes!
