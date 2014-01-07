$:.unshift('./lib')
require 'pg/em/connection_pool'
require 'em-synchrony'
require 'em-synchrony/fiber_iterator'
require 'pp'
require 'benchmark'

TABLE_NAME = 'resources'
LIMIT_ROWS = 5000

include EM::Synchrony

unless PG::EM::Client.single_row_mode?
  raise 'compile pg against pqlib >= 9.2 to support single row mode'
end

def benchmark(repeat=40)
  Benchmark.bm(20) do |b|
    puts
    b.report("threads #{repeat/1}x1:")   { threads(repeat, 1) }
    b.report("threads #{repeat/5}x5:")   { threads(repeat, 5) }
    b.report("threads #{repeat/10}x10:") { threads(repeat, 10) }
    b.report("threads #{repeat/20}x20:") { threads(repeat, 20) }
    b.report("threads #{repeat/40}x40:") { threads(repeat, 40) }
    puts
    b.report("fibers  #{repeat/1}x1:")   { fibers(repeat, 1) }
    b.report("fibers  #{repeat/5}x5:")   { fibers(repeat, 5) }
    b.report("fibers  #{repeat/10}x10:") { fibers(repeat, 10) }
    b.report("fibers  #{repeat/20}x20:") { fibers(repeat, 20) }
    b.report("fibers  #{repeat/40}x40:") { fibers(repeat, 40) }
  end
end

def threads(repeat, concurrency)
  db = Hash.new { |pool, id| pool[id] = PG::Connection.new }
  (repeat/concurrency).times do
    (0...concurrency).map do |i|
      Thread.new do
        stream_results(db[i])
      end
    end.each(&:join)
  end
  db.each_value(&:finish).clear
end

def fibers(repeat, concurrency)
  EM.synchrony do
    db = PG::EM::ConnectionPool.new size: concurrency, lazy: true
    (repeat/concurrency).times do
      FiberIterator.new((0...concurrency), concurrency).each do
        db.hold do |pg|
          stream_results(pg)
        end
      end
    end
    db.finish
    EM.stop
  end
end

def stream_results(pg)
  pg.send_query("select * from #{TABLE_NAME}")
  pg.set_single_row_mode
  rows = 0
  last_time = Time.now
  while result = pg.get_result
    begin
      result.check
      result.each do |tuple|
        rows += 1
        if rows >= LIMIT_ROWS
          pg.reset
          break
        end
      end
    rescue PG::Error => e
      pg.get_last_result
      raise e
    ensure
      result.clear
    end
  end
end

if $0 == __FILE__
  benchmark ARGV[0].to_i.nonzero? || 40
end

