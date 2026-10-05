# First Post

A sample blog post rendered by the Croupier SSG example.

## Why So Simple?

The generator is a demonstration of incremental builds, not of
blogging: each markdown file in `content/` becomes one HTML page,
and only the pages whose sources changed are re-rendered on the
next run.

## Markdown Features

Code:

```crystal
puts "rendered by croupier"
```

Lists, *emphasis*, **strong** text, and

> block quotes

all render through [markd](https://github.com/icyleaf/markd).
