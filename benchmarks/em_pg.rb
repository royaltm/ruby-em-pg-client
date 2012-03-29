$:.unshift('./lib')
require 'eventmachine'
require 'em-synchrony'
require 'em-synchrony/pg'
require "em-synchrony/fiber_iterator"
require 'em-synchrony/transaction'
require 'pp'
require 'benchmark'

$dbname = 'alpha'

def benchmark(repeat=100)
  Benchmark.bm(20) do |b|
    b.report('single:')         { single(repeat) }
    puts
    b.report('parallel 90000/1:')  { parallel(repeat, 90000, 1) }
    b.report('parallel 5000/5:')  { parallel(repeat, 5000, 5) }
    b.report('parallel 2000/10:') { parallel(repeat, 2000, 10) }
    b.report('parallel 1000/20:') { parallel(repeat, 1000, 20) }
    puts
    patch_blocking
    b.report('blocking 90000/1:')  { parallel(repeat, 90000, 1) }
    b.report('blocking 5000/5:')  { parallel(repeat, 5000, 5) }
    b.report('blocking 2000/10:') { parallel(repeat, 2000, 10) }
    b.report('blocking 1000/20:') { parallel(repeat, 1000, 20) }
    patch_remove_blocking
  end
end

def patch_remove_blocking
  PG::EM::Client::Watcher.module_eval <<-EOE
    alias_method :notify_readable, :original_notify_readable
    undef :original_notify_readable
  EOE
end

def patch_blocking
  PG::EM::Client::Watcher.module_eval <<-EOE
    alias_method :original_notify_readable, :notify_readable
    def notify_readable
      detach
      begin
        result = @client.get_last_result
      rescue Exception => e
        @deferrable.fail(e)
      else
        @deferrable.succeed(result)
      end
    end
  EOE
end

# retrieve resources using single select query
def single(repeat=1)
  rowcount = 0
  p = PGconn.new :dbname => $dbname
  p.query('select count(*) from resources') do |result|
    rowcount = result.getvalue(0,0).to_i
  end
  repeat.times do
    p.query('select * from resources order by cdate') do |result|
      $resources = result.values
    end
  end
  # raise "invalid count #{$resources.length} != #{rowcount}" if $resources.length != rowcount
end

# retrieve resources using parallel queries
def parallel(repeat=1, chunk_size=2000, concurrency=10)
  resources = []
  rowcount = 0
  EM.synchrony do
    p = EM::Synchrony::ConnectionPool.new(size: concurrency) { PG::EM::Client.new :dbname => $dbname }
    p.query('select count(*) from resources') do |result|
      rowcount = result.getvalue(0,0).to_i
    end
    offsets = (rowcount / chunk_size.to_f).ceil.times.map {|n| n*chunk_size }
    repeat.times do
      EM::Synchrony::FiberIterator.new(offsets, concurrency).each do |offset|
        p.query('select * from resources order by cdate limit $1 offset $2', [chunk_size, offset]) do |result|
          resources[offset, chunk_size] = result.values
        end    
      end
    end
    EM.stop
  end
  # raise "invalid count #{resources.length} != #{rowcount}" if resources.length != rowcount
  # raise "resources != $resources" if resources != $resources
  resources
end

if $0 == __FILE__
  benchmark (ARGV.first || 10).to_i
end
