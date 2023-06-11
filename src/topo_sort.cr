# Algorithm shamelessly stolen from https://stackoverflow.com/a/47234034
# Thanks Blckknght!

def topological_sort(g)
  seen = Set(String).new
  stack = Array(String).new
  order = Array(String).new
  q = ["start"]
  while !q.empty?
    v = q.pop
    if !seen.includes?(v)
      seen << v
      g[v].each do |w|
        q << w
      end
      while !stack.empty? && !g[stack.last].includes?(v)
        order << stack.pop
      end
      stack << v
    end
  end
  result = stack + order.reverse
  if result.size != g.size
    raise "Cycle detected"
  end
  result
end
