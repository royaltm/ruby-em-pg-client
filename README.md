em-pg-client
============

Author: Rafał Michalski  (rafal at yeondir dot com)

* http://github.com/royaltm/ruby-em-pg-client

Description
-----------

__em-pg-client__ is the Ruby and EventMachine driver interface to the
PostgreSQL RDBMS. It is based on [ruby-pg](https://bitbucket.org/ged/ruby-pg).

Features
--------

* Non-blocking / fully asynchronous processing with EventMachine,
* event reactor auto-detecting, asynchronous fiber-entangling command methods
  (the same code can be used regardless of the EventMachine reactor state)
* additional asynchronous deferrable command methods,
* fully asynchronous automatic re-connects on connection losses
  (e.g.: RDBMS restarts, network failures),
* minimal changes to [PG::Connection](http://deveiate.org/code/pg/PG/Connection.html) API,
* configurable timeouts (connect or execute) of asynchronous processing,
* dedicated connection pool with dynamic size, supporing all asynchronous
  client methods and transactions,
* [Sequel Adapter](https://github.com/fl00r/em-pg-sequel) by Peter Yanovich,
* works on windows (requires ruby 2.0) (issue #7)

Bugs/Limitations
----------------

* no async support for: COPY commands (`get_copy_data`,  `put_copy_data`),
  `wait_for_notify` and `transaction`
* actually no ActiveRecord support (you are welcome to contribute).

API Changes between versions
----------------------------

### 0.2.x -> 0.3.x

There is a substantial API interface change between this and the previous
version releases. The idea was to make this implementation as much compatible
as possible with the `pg` interface.
E.g. the `#async_exec` is now an alias to `#exec`.

The other reason was to get rid of the ugly em / em-synchrony duality.

* There is no separate em-synchrony client version anymore.
* The methods returning Deferrable ha`ve now the `*_defer` suffix.
* The `#async_exec` and `#async_query` (in <= 0.2 they were deferrable methods)
  are now aliases to `#exec`.
* The `pg` command methods `#exec`, `#query`, `#exec_*`, `#describe_*` now behave
  the same as in `em-synchrony/pg` <= 0.2 client.
* The following methods were removed:

  - `#async_prepare`,
  - `#async_exec_prepared`,
  - `#async_describe_prepared`,
  - `#async_describe_portal`

  as their names were confusing due to the unfortunate `#async_exec`.

* The `async_connect` and `#async_reset` are renamed to `connect_defer` and `#reset_defer`
  respectively and are now deprecated aliases.

### 0.1.x -> 0.2.x

* `on_reconnect` renamed to more accurate `on_autoreconnect`
  (well, it's not used by PG::EM::Client#reset call).
* `async_autoreconnect` is `false` by default if `on_autoreconnect`
  is __not__ specified as initialization option.

TODO:
-----

* implement streaming results (Postgres >= 9.2) in some way
* implement EM adapted version of `get_copy_data`, `put_copy_data`,
  `wait_for_notify` and `transaction`
* ORM (ActiveRecord and maybe Datamapper) support as separate projects
* present more benchmarks

Requirements
------------

* ruby >= 1.9 (tested: 1.9.3-p374, 1.9.2-p320, 1.9.1-p378, 2.0.0-p353, 2.1.0)
* https://bitbucket.org/ged/ruby-pg >= 0.17.0
* [PostgreSQL](http://www.postgresql.org/ftp/source/) RDBMS >= 8.3
* http://rubyeventmachine.com >= 1.0.0
*  [EM-Synchrony](https://github.com/igrigorik/em-synchrony)
  (optional - not needed for any of the client functionality,
  just wrap your code in a fiber)

Install
-------

```
  $ [sudo] gem install em-pg-client
```

#### Gemfile

```ruby
  gem "em-pg-client", "~> 0.3.0"
```

#### Github

```
  git clone git://github.com/royaltm/ruby-em-pg-client.git
```

Why?
----

Because I didn't find any ruby-pg's EM implementation to fit my needs.
I've found at least 3 other implementations of EM postgres client:

* https://github.com/jzimmek/em-postgresql-sequel
* https://github.com/leftbee/em-postgresql-adapter
* https://github.com/jtoy/em-postgres

and (except the EM-bundled one which uses no longer maintained postgres-pr library)
all of them have similiar flaws:

* 2 of them are designed to support some ORM (ActiveRecord or Sequel),
  so they are EM-Synchrony only,
* non-standard API method names,
* no (nonexistent or non-working) autoreconnect implementation,
* poor error handling,
* not fully supporting asynchronous PG::Connection API.

The last one is worth a comment:

They all use blocking methods to retrieve whole result from server
([PGConn#block](http://deveiate.org/code/pg/PG/Connection.html#method-i-block) or
[PGConn#get_result](http://deveiate.org/code/pg/PG/Connection.html#method-i-get_result) which also
blocks when there is not enough buffered data on socket).

This implementation makes use of non-blocking:
[PGConn#is_busy](http://deveiate.org/code/pg/PG/Connection.html#method-i-is_busy) and
[PGConn#consume_input](http://deveiate.org/code/pg/PG/Connection.html#method-i-consume_input) methods.
Depending on the size of queries results and the level of concurrency, the gain in overall speed and
responsiveness of your application might be actually quite huge. I've done some
{file:BENCHMARKS.md BENCHMARKING} already.

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

Usage
-----
__em-pg-client__ provides {PG::EM::Client} class which inherits
[PG::Connection](http://deveiate.org/code/pg/PG/Connection.html).
You can work with {PG::EM::Client} almost the same way you would with
PG::Connection.

The real difference begins when you turn EventMachine reactor on.

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

### PG::Connection methods adapted to EventMachine

The list of PG::EM::Client async methods for processing with EventMachine.

#### Asynchronous, returning `EM::Deferrable` methods:

* `Client.connect_defer` (singleton method)
* `reset_defer`
* `exec_defer` (alias: `query_defer`)
* `prepare_defer`
* `exec_prepared_defer`
* `describe_prepared_defer`
* `describe_portal_defer`

For arguments of theese methods consult their original blocking (without `_defer` suffix)
counterparts in [PG::Connection](http://deveiate.org/code/pg/PG/Connection.html) manual.

Use `callback` on the returned `Deferrable` to receive result. 
The result you receive is PG::EM::Client for PG::EM::Client.async_connect
and `async_reset`, and the [PG::Result](http://deveiate.org/code/pg/PG/Result.html)
for the rest of the methods. The received PG::EM::Client is in a connected state
and ready for queries. You need to `clear` obtained PG::Result object yourself
or leave it to `gc`.

To detect a failure in an executed method use `errback` on returned `Deferrable`.
You should expect an instance of `Exception` (usually PG::Error) as `errback`
argument. You may check its `backtrace` to find origin of the error.

#### Reactor sensing, asynchronous but synchronized to fiber methods:

The list of PG::EM::Client fiber aware methods for processing with
EM-Synchrony / EventMachine.

* `Client.new` (singleton, alias: `connect`, `open`, `setdb`, `setdblogin`)
* `reset`
* `exec` (alias: `query`, `async_exec`, `async_query`)
* `prepare`
* `exec_prepared`
* `describe_prepared`
* `describe_portal`

Under the hood, the above methods call `*_defer` counterparts of themselves
and `yield` from the current fiber awaiting for the result. The PG::Result
(or PG::EM::Client for `connect` and `reset`) is then returned to the caller.
If the code block was given, it is called with the result as its argument.
In that case the value of the block is returned instead and PG::Result is
cleared (or in case of `connect` or `reset` PG::EM::Client is being closed)
after executing block.

These methods detect if EventMachine is running and if the current fiber is
not a root fiber the parent (blocking) PG::Connection methods are called
instead.

Like in pure EventMachine version you can mix asynchronous, fiber aware and
blocking methods without finishing the connection. You only need to
start/stop EventMachine in between asynchronous calls.

Although [em-synchrony](https://github.com/igrigorik/em-synchrony/) provides
very nice set of tools for untangled EventMachine, you don't really require
it to fully benefit from the PG::EM::Client. Just wrap your asynchronous
code in a fiber:

    Fiber.new { ... }.resume

#### Special options

There are 3 additional connection options and one standard `pg` option used by
async methods. You may add them as one of the __hash__ options to
{PG::EM::Client.new} or {PG::EM::Client.async_connect} or simply use accessor
methods to change them on the fly. The additional options are not passed to
`libpq`.

The options are:

* `async_autoreconnect`
* `on_autoreconnect`
* `query_timeout`
* `connect_timeout`

Only `connect_timeout` is a standard `libpq` option, although changing it by
accessor method only affects asynchronous functions.
See {PG::EM::Client} for the details.

#### Handling errors

Exactly like in `pg`:

```ruby
  EM.synchrony do
    begin
      pg.query('sellect * from foo')
    rescue PG::SyntaxError => e
      puts "PSQL error: #{e.inspect}"
    end
    EM.stop
  end
```

### Auto re-connecting in asynchronous mode

Connection reset is done in a non-blocking manner using `reset_defer` internally.

```ruby
  EM.run do
    Fiber.new do
      pg = PG::EM::Client.new dbname: 'test',
            async_autoreconnect: true
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

It's also possible to define `on_autoreconnect` callback to be invoked
while the connection has been reset. It's called just before the send query
command is executed:

```ruby
  EM.run do
    Fiber.new do
      pg = PG::EM::Client.new dbname: 'test',
            async_autoreconnect: true
      pg.on_autoreconnect = proc do |c, e|
        c.prepare('bar', 'select * from foo order by cdate desc')
      end
      pg.on_autoreconnect.call pg

      try_query = lambda do
        pg.exec_prepared('bar') do |result|
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

It's possible to send queries from inside of the `on_autoreconnect` as in
the above example. See {PG::EM::Client#on_autoreconnect} docs for details.

### {PG::EM::ConnectionPool}

Forever alone? Not anymore! There is a dedicated connection pool class which
can asynchronously create new connections on concurrent usage and automatically
drop the failed ones.

It also provides a #transaction method which locks the in-transaction
connection to the calling fiber and allows to execute commands
on the same connection within a transaction block.
The transactions may be nested thanks to the enhancend
{PG::EM::Client#transaction} method.

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
