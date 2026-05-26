const test = require('brittle')
const { Readable, Writable } = require('../')

test('passes data from resolved stream', (t) => {
  t.plan(2)

  const out = Readable.deferred(() => Promise.resolve(Readable.from(['a', 'b', 'c'])))
  const chunks = []

  out.on('data', (d) => chunks.push(d))
  out.on('close', () => {
    t.alike(chunks, ['a', 'b', 'c'])
    t.ok(out.destroyed)
  })
})

test('ends cleanly when fn returns null', (t) => {
  t.plan(2)

  const out = Readable.deferred(() => Promise.resolve(null))

  let ended = 0
  out.on('end', () => ended++)
  out.on('close', () => {
    t.is(ended, 1)
    t.ok(out.destroyed)
  })
  out.resume()
})

test('async fn awaits before piping', (t) => {
  t.plan(1)

  const out = Readable.deferred(async () => {
    await new Promise((resolve) => setTimeout(resolve, 10))
    return Readable.from([1, 2, 3])
  })

  const chunks = []
  out.on('data', (d) => chunks.push(d))
  out.on('close', () => t.alike(chunks, [1, 2, 3]))
})

test('rejected fn destroys output', (t) => {
  t.plan(1)

  const out = Readable.deferred(() => Promise.reject(new Error('fn failed')))
  out.on('error', (err) => t.is(err.message, 'fn failed'))
  out.resume()
})

test('error in inner stream destroys output', (t) => {
  t.plan(1)

  const inner = new Readable({
    open(cb) {
      cb(new Error('inner failed'))
    }
  })
  const out = Readable.deferred(() => Promise.resolve(inner))
  out.on('error', (err) => t.is(err.message, 'inner failed'))
  out.resume()
})

test('destroying output before fn resolves does not crash', (t) => {
  t.plan(1)

  const out = Readable.deferred(async () => {
    await new Promise((resolve) => setTimeout(resolve, 20))
    return Readable.from([1, 2, 3])
  })

  out.on('close', () => t.ok(out.destroyed))
  out.destroy()
})

test('pipes into a writable correctly', (t) => {
  t.plan(1)

  const out = Readable.deferred(() => Promise.resolve(Readable.from(['x', 'y', 'z'])))
  const collected = []

  const sink = new Writable({
    write(data, cb) {
      collected.push(data)
      cb(null)
    }
  })

  sink.on('finish', () => t.alike(collected, ['x', 'y', 'z']))
  out.pipe(sink)
})
