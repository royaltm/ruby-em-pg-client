em-pg-client
============

The Ruby EventMachine driver interface to the PostgreSQL RDBMS. It is based on
[ruby-pg](https://bitbucket.org/ged/ruby-pg).

[![Gem Version][GV img]][Gem Version]
[![Dependency Status][DS img]][Dependency Status]
[![Coverage Status][CS img]][Coverage Status]
[![Build Status][BS img]][Build Status]

Author: Rafał Michalski (rafal at yeondir dot com)

* http://github.com/royaltm/ruby-em-pg-client

Description
-----------

__em-pg-client__ provides {PG::EM::Client} class which inherits
[PG::Connection](http://deveiate.org/code/pg/PG/Connection.html).
You can work with {PG::EM::Client} almost the same way you would work
with PG::Connection.

The real difference begins when you turn the EventMachine reactor on.

```ruby
  require 'pg/em'
  
  pg = PG::EM::Client.new dbname: 'test'

  # no async
  pg.query('select * from foo') do |result|
    puts Array(result).inspect
  end

  # asynchronous
  EM.run do
    Fiber.new do
      pg.query('select * from foo') do |result|
        puts Array(result).inspect
      end
      EM.stop
    end.resume
  end

  # asynchronous + deferrable
  EM.run do
    df = pg.query_defer('select * from foo')
    df.callback { |result|
      puts Array(result).inspect
      EM.stop
    }
    df.errback {|ex|
      raise ex
    }
    puts "sent"
  end
```

Features
--------

* Non-blocking / fully asynchronous processing with EventMachine.
* Event reactor auto-detecting, asynchronous fiber-synchronized command methods
  (the same code can be used regardless of the EventMachine reactor state)
* Asynchronous EM-style (Deferrable returning) command methods.
* Fully asynchronous automatic re-connects on connection failures
  (e.g.: RDBMS restarts, network failures).
* Minimal changes to [PG::Connection](http://deveiate.org/code/pg/PG/Connection.html) API.
* Configurable timeouts (connect or execute) of asynchronous processing.
* Dedicated connection pool with dynamic size, supporting asynchronous
  processing and transactions.
* [Sequel Adapter](https://github.com/fl00r/em-pg-sequel) by Peter Yanovich.
* Works on windows (requires ruby 2.0) ([issue #7][Issue 7]).
* Supports asynchronous query data processing in single row mode
  ([issue #12][Issue 12]). See {file:BENCHMARKS.md BENCHMARKING}.
* __New__ - asynchronous implementation of wait_for_notify

Requirements
------------

* ruby >= 1.9.3
* https://bitbucket.org/ged/ruby-pg >= 0.17.0
* [PostgreSQL](http://www.postgresql.org/ftp/source/) RDBMS >= 9.1
* http://rubyeventmachine.com >= 1.2.0
*  [EM-Synchrony](https://github.com/igrigorik/em-synchrony)
  (optional - not needed for any of the client functionality,
  just wrap your code in a fiber)

Install
-------

```sh
  $ [sudo] gem install em-pg-client
```

#### Gemfile

```ruby
  gem "em-pg-client", "~> 0.3.4"
```

#### Github

```
  git clone git://github.com/royaltm/ruby-em-pg-client.git
```

Usage
-----

### PG::Connection commands adapted to the EventMachine

#### Asynchronous, the EventMachine style:

* `Client.connect_defer` (singleton method)
* `reset_defer`
* `exec_defer` (alias: `query_defer`)
* `prepare_defer`
* `exec_prepared_defer`
* `describe_prepared_defer`
* `describe_portal_defer`

For arguments of these methods consult their original (without the `_defer`
suffix) counterparts in the
[PG::Connection](http://deveiate.org/code/pg/PG/Connection.html) manual.

Use `callback` with a block on the returned deferrable object to receive the
result. In case of `connect_defer` and `reset_defer` the result is an instance
of the {PG::EM::Client}. The received client is in connected state and ready
for the queries. Otherwise an instance of the
[PG::Result](http://deveiate.org/code/pg/PG/Result.html) is received. You may
`clear` the obtained result object or leave it to `gc`.

To detect an error in the executed command call `errback` on the deferrable
with a block. You should expect an instance of the raised `Exception`
(usually PG::Error) as the block argument.

#### Reactor sensing methods, EM-Synchrony style:

* `Client.new` (singleton, alias: `connect`, `open`, `setdb`, `setdblogin`)
* `reset`
* `exec` (alias: `query`, `async_exec`, `async_query`)
* `prepare`
* `exec_prepared`
* `describe_prepared`
* `describe_portal`

The above methods call `*_defer` counterparts of themselves and `yield`
from the current fiber awaiting for the result. The PG::Result instance
(or PG::EM::Client for `new`) is then returned to the caller.
If a code block is given, it will be passed the result as an argument.
In that case the value of the block is returned instead and the result is
being cleared (or in case of `new` - client is being closed) after block
terminates.

These methods check if EventMachine's reactor is running and the current fiber
is not a root fiber. Otherwise the parent (thread-blocking) PG::Connection
methods are being called.

You can call asynchronous, fiber aware and blocking methods without finishing
the connection. You only need to start/stop EventMachine in between the
asynchronous calls.

Although the [em-synchrony](https://github.com/igrigorik/em-synchrony/)
provides very nice set of tools for the untangled EventMachine, you don't
really require it to fully benefit from the PG::EM::Client. Just wrap your
asynchronous code in a fiber:

    Fiber.new { ... }.resume

#### Special options

There are four special connection options and one of them is a standard `pg`
option used by the async methods. You may pass them as one of the __hash__
options to {PG::EM::Client.new} or {PG::EM::Client.connect_defer} or simply
use the accessor methods to change them on the fly.

The options are:

* `connect_timeout`
* `query_timeout`
* `async_autoreconnect`
* `on_autoreconnect`
* `on_connect`

Only `connect_timeout` is a standard `libpq` option, although changing it with
the accessor method affects asynchronous functions only.
See {PG::EM::Client} for more details.

#### Handling errors

Exactly like in `pg`:

```ruby
  EM.synchrony do
    begin
      pg.query('smellect 1')
    rescue => e
      puts "error: #{e.inspect}"
    end
    EM.stop
  end
```

with *_defer methods:

```ruby
  EM.run do
    pg.query_defer('smellect 1') do |ret|
      if ret.is_a?(Exception)
        puts "PSQL error: #{ret.inspect}"
      end
    end
  end
```

or

```ruby
  EM.run do
    pg.query_defer('smellect 1').callback do |ret|
      puts "do something with #{ret}"
    end.errback do |err|
      puts "PSQL error: #{err.inspect}"
    end
  end
```

### Auto re-connecting in asynchronous mode

Connection reset is done in a non-blocking manner using `reset_defer` internally.

```ruby
  EM.run do
    Fiber.new do
      pg = PG::EM::Client.new async_autoreconnect: true

      try_query = lambda do
        pg.query('select * from foo') do |result|
          puts Array(result).inspect
        end
      end

      try_query.call
      system 'pg_ctl stop -m fast'
      system 'pg_ctl start -w'
      try_query.call

      EM.stop
    end.resume
  end
```

to enable this feature call:

```ruby
  pg.async_autoreconnect = true
```

Additionally the `on_autoreconnect` callback may be set on the connection.
It's being invoked after successfull connection restart, just before the
pending command is sent again to the server.

### Server-sent notifications - async style

Not surprisingly, there are two possible ways to wait for notifications,
one with a deferrable:

```ruby
  pg = PG::EM::Client.new
  EM.run do
    pg.wait_for_notify_defer(7).callback do |notify|
      if notify
        puts "Someone spoke to us on channel: #{notify[:relname]} from #{notify[:be_pid]}"
      else
        puts "Too late, 7 seconds passed"
      end
    end.errback do |ex|
      puts "Connection to deep space lost..."
    end
    pg.query_defer("LISTEN deep_space") do
      pg.query_defer("NOTIFY deep_space") do
        puts "Reaching out... to the other worlds"
      end
    end
  end
```

and the other, using fibers:

```ruby
  EM.synchrony do
    pg = PG::EM::Client.new
    EM::Synchrony.next_tick do
      pg.query('LISTEN "some channel"')
      pg.query('SELECT pg_notify($1::text,$2::text)', ['some channel', 'with some message'])
    end
    pg.wait_for_notify(10) do |channel, pid, payload|
      puts "I've got notification on #{channel} #{payload}."
    end.tap do |name|
      puts "Whatever, I've been waiting too long already" if name.nil?
    end
  end
```

As you might have noticed, one does not simply wait for notifications,
but one can also run some queries on the same connection at the same time,
if one wishes so.

### Connection Pool

Forever alone? Not anymore! There is a dedicated {PG::EM::ConnectionPool}
class with dynamic pool for both types of asynchronous commands (deferral
and fiber-synchronized).

It also provides a #transaction method which locks the in-transaction
connection to the calling fiber and allows to execute commands
on the same connection within a transaction block. The transactions may
be nested. See also docs for the {PG::EM::Client#transaction} method.

#### Parallel async queries

```ruby
  require 'pg/em/connection_pool'
  require 'em-synchrony'

  EM.synchrony do
    pg = PG::EM::ConnectionPool.new(size: 2, dbname: 'test')

    multi = EM::Synchrony::Multi.new
    multi.add :foo, pg.query_defer('select pg_sleep(1)')
    multi.add :bar, pg.query_defer('select pg_sleep(1)')

    start = Time.now
    res = multi.perform
    # around 1 sec.
    puts Time.now - start

    EM.stop
  end
```

#### Fiber Concurrency

```ruby
  require 'pg/em/connection_pool'
  require 'em-synchrony'
  require "em-synchrony/fiber_iterator"

  EM.synchrony do
    concurrency = 5
    queries = (1..10).map {|i| "select pg_sleep(1); select #{i}" }

    pg = PG::EM::ConnectionPool.new(size: concurrency, dbname: 'test')

    start = Time.now
    EM::Synchrony::FiberIterator.new(queries, concurrency).each do |query|
      pg.query(query) do |result|
        puts "recv: #{result.getvalue(0,0)}"
      end
    end
    # around 2 secs.
    puts Time.now - start

    EM.stop
  end
```

API Changes
-----------

### 0.2.x -> 0.3.x

There is a substantial difference in the API between this and the previous
releases. The idea behind it was to make this implementation as much
compatible as possible with the threaded `pg` interface.
E.g. the `#async_exec` is now an alias to `#exec`.

The other reason was to get rid of the ugly em / em-synchrony duality.

* There is no separate em-synchrony client version anymore.
* The methods returning Deferrable have now the `*_defer` suffix.
* The `#async_exec` and `#async_query` (in <= 0.2 they were deferrable methods)
  are now aliases to `#exec`.
* The command methods `#exec`, `#query`, `#exec_*`, `#describe_*` are now
  em-synchrony style methods (fiber-synchronized).
* The following methods were removed:

    - `#async_prepare`,
    - `#async_exec_prepared`,
    - `#async_describe_prepared`,
    - `#async_describe_portal`

  as their names were confusing due to the unfortunate `#async_exec`.

* The `async_connect` and `#async_reset` are renamed to `connect_defer` and `#reset_defer`
  respectively.

### 0.1.x -> 0.2.x

* `on_reconnect` renamed to more accurate `on_autoreconnect`
  (well, it's not used by PG::EM::Client#reset call).
* `async_autoreconnect` is `false` by default if `on_autoreconnect`
  is __not__ specified as initialization option.

Bugs/Limitations
----------------

* no async support for COPY commands (`get_copy_data`,  `put_copy_data`)
* actually no ActiveRecord support (you are welcome to contribute).

TODO:
-----

* more convenient streaming API
* implement EM adapted version of `get_copy_data`, `put_copy_data`
* ORM (ActiveRecord and maybe Datamapper) support as separate projects

More Info
---------

This implementation makes use of non-blocking:
[PGConn#is_busy](http://deveiate.org/code/pg/PG/Connection.html#method-i-is_busy) and
[PGConn#consume_input](http://deveiate.org/code/pg/PG/Connection.html#method-i-consume_input) methods.
Depending on the size of queried results and the concurrency level, the gain
in overall speed and responsiveness of your application might be actually quite huge.
See {file:BENCHMARKS.md BENCHMARKING}.

Thanks
------

The greetz go to:

* [Authors](https://bitbucket.org/ged/ruby-pg/wiki/Home#!copying) of __pg__
  driver (especially for its async-api)
* Francis Cianfrocca for great reactor framework
  [EventMachine](https://github.com/eventmachine/eventmachine)
* Ilya Grigorik [igrigorik](https://github.com/igrigorik) for
  [untangling EM with Fibers](http://www.igvita.com/2010/03/22/untangling-evented-code-with-ruby-fibers/)
* Peter Yanovich [fl00r](https://github.com/fl00r) for the
  [em-pg-sequel](https://github.com/fl00r/em-pg-sequel)
* Andrew Rudenko [prepor](https://github.com/prepor) for the implicit idea
  of the re-usable watcher from his [em-pg](https://github.com/prepor/em-pg).

[Gem Version]: https://rubygems.org/gems/em-pg-client
[Dependency Status]: https://gemnasium.com/royaltm/ruby-em-pg-client
[Coverage Status]: https://coveralls.io/r/royaltm/ruby-em-pg-client
[Build Status]: https://travis-ci.org/royaltm/ruby-em-pg-client
[Issue 7]: https://github.com/royaltm/ruby-em-pg-client/issues/7
[Issue 12]: https://github.com/royaltm/ruby-em-pg-client/issues/12
[GV img]: https://badge.fury.io/rb/em-pg-client.png
[DS img]: https://gemnasium.com/royaltm/ruby-em-pg-client.png
[CS img]: https://coveralls.io/repos/royaltm/ruby-em-pg-client/badge.png
[BS img]: https://travis-ci.org/royaltm/ruby-em-pg-client.png
[BB img]: https://d2weczhvl823v0.cloudfront.net/royaltm/ruby-em-pg-client/trend.png
