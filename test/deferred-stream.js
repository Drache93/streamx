const test = require('brittle')
const { Readable, Writable } = require('../')

test('passes data from inner stream', (t) => {
  t.plan(2)

  const inner = Readable.from(['a', 'b', 'c'])
  const trigger = new Readable({
    open(cb) {
      this.push(inner)
      this.push(null)
      cb(null)
    }
  })

  const out = Readable.DeferredStream(trigger)
  const chunks = []

  out.on('data', (d) => chunks.push(d))
  out.on('close', () => {
    t.alike(chunks, ['a', 'b', 'c'])
    t.ok(out.destroyed)
  })
})

test('ends cleanly when trigger emits nothing', (t) => {
  t.plan(2)

  const trigger = new Readable({
    open(cb) {
      this.push(null)
      cb(null)
    }
  })

  const out = Readable.DeferredStream(trigger)

  let ended = 0
  out.on('end', () => ended++)
  out.on('close', () => {
    t.is(ended, 1)
    t.ok(out.destroyed)
  })
  out.resume()
})

test('async open resolves before piping', (t) => {
  t.plan(1)

  const inner = Readable.from([1, 2, 3])
  const trigger = new Readable({
    async open(cb) {
      await new Promise((resolve) => setTimeout(resolve, 10))
      this.push(inner)
      this.push(null)
      cb(null)
    }
  })

  const out = Readable.DeferredStream(trigger)
  const chunks = []

  out.on('data', (d) => chunks.push(d))
  out.on('close', () => t.alike(chunks, [1, 2, 3]))
})

test('error in trigger stream destroys output', (t) => {
  t.plan(1)

  const trigger = new Readable({
    open(cb) {
      cb(new Error('trigger failed'))
    }
  })

  const out = Readable.DeferredStream(trigger)
  out.on('error', (err) => t.is(err.message, 'trigger failed'))
  out.resume()
})

test('error in inner stream destroys output', (t) => {
  t.plan(1)

  const inner = new Readable({
    open(cb) {
      cb(new Error('inner failed'))
    }
  })

  const trigger = new Readable({
    open(cb) {
      this.push(inner)
      this.push(null)
      cb(null)
    }
  })

  const out = Readable.DeferredStream(trigger)
  out.on('error', (err) => t.is(err.message, 'inner failed'))
  out.resume()
})

test('destroying output before trigger resolves does not crash', (t) => {
  t.plan(1)

  const trigger = new Readable({
    async open(cb) {
      await new Promise((resolve) => setTimeout(resolve, 20))
      this.push(Readable.from([1, 2, 3]))
      this.push(null)
      cb(null)
    }
  })

  const out = Readable.DeferredStream(trigger)
  out.on('close', () => t.ok(out.destroyed))
  out.destroy()
})

test('pipes into a writable correctly', (t) => {
  t.plan(1)

  const inner = Readable.from(['x', 'y', 'z'])
  const trigger = new Readable({
    open(cb) {
      this.push(inner)
      this.push(null)
      cb(null)
    }
  })

  const out = Readable.DeferredStream(trigger)
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
