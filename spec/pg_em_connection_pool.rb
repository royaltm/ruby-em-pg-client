$:.unshift "lib"
gem 'eventmachine', '~> 1.0.0'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'eventmachine'
require 'em-synchrony'
require 'pg/em/connection_pool'
require 'connection_pool_helpers'

RSpec.configure do |c|
  c.include ConnectionPoolHelpers
end

describe PG::EM::ConnectionPool do
  subject            { PG::EM::ConnectionPool }

  let(:client)       { Class.new }
  let(:deferrable)   { PG::EM::FeaturedDeferrable }
  let(:checkpoint)   { proc {} }
  let(:pgerror)      { PG::Error.new }
  let(:fooerror)     { RuntimeError.new 'foo' }
  let(:timeout)      { 10 }
  let(:hook)         { proc {|c| }}

  it "should allocate one connection" do
    client.should_receive(:new).with({}).once.and_return(client.allocate)
    pool = subject.new connection_class: client
    pool.should be_an_instance_of subject
    pool.max_size.should eq subject::DEFAULT_SIZE
    pool.available.length.should eq 1
    pool.allocated.length.should eq 0
    pool.size.should eq 1
    pool.available.first.should be_an_instance_of client
    pool.available.first.should_receive(:finish).once
    pool.finish.should be pool
    pool.max_size.should eq subject::DEFAULT_SIZE
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
  end

  it "should asynchronously allocate one connection" do
    checkpoint.should_receive(:call) do |pool|
      pool.should be_an_instance_of subject
      pool.max_size.should eq 42
      pool.available.length.should eq 1
      pool.allocated.length.should eq 0
      pool.size.should eq 1
      pool.available.first.should be_an_instance_of client
    end
    client.should_receive(:connect_defer).with({}).once.and_return(
      deferrable.new.tap {|df| df.succeed client.allocate }
    )
    df = subject.connect_defer connection_class: client, size: 42
    df.should be_an_instance_of deferrable
    df.callback(&checkpoint)
  end

  it "should write to client attributes and finish" do
    client.should_receive(:new) do |options|
      options.should be_empty
      client.allocate.tap do |conn|
        conn.should_receive(:connect_timeout=).with(timeout).once
        conn.should_receive(:query_timeout=).with(timeout).once
        conn.should_receive(:on_autoreconnect=).with(hook).twice
        conn.should_receive(:on_connect=).with(hook).twice
        conn.should_receive(:finish).once
      end
    end.exactly(3)
    pool = subject.new connection_class: client, size: 3
    pool.should be_an_instance_of subject
    pool.connect_timeout.should be_nil
    pool.query_timeout.should be_nil
    pool.max_size.should eq 3
    pool.available.length.should eq 1
    pool.allocated.length.should eq 0
    pool.size.should eq 1
    pool.hold do
      pool.size.should eq 1
      Fiber.new do
        pool.hold do
          pool.size.should eq 2
          Fiber.new do
            pool.hold do
              pool.available.length.should eq 0
              pool.allocated.length.should eq 3
              pool.size.should eq 3
            end
          end.resume
          pool.available.length.should eq 1
          pool.allocated.length.should eq 2
          pool.connect_timeout = timeout
          pool.query_timeout = timeout
          pool.on_connect = hook
          pool.on_connect.should be hook
          pool.on_connect(&hook)
          pool.on_autoreconnect = hook
          pool.on_autoreconnect.should be hook
          pool.on_autoreconnect(&hook)
        end
      end.resume
    end
    pool.available.length.should eq 3
    pool.allocated.length.should eq 0
    pool.size.should eq 3
    pool.connect_timeout.should eq timeout
    pool.query_timeout.should eq timeout
    pool.finish.should be pool
    pool.max_size.should eq 3
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.on_connect.should be hook
    pool.on_autoreconnect.should be hook
    pool.size.should eq 0
  end

  it "should allocate new connection with altered attributes" do
    client.should_receive(:new).with(
      {connect_timeout: timeout, query_timeout: timeout,
       on_connect: hook, on_autoreconnect: hook}
    ).once.and_return(client.allocate)
    pool = subject.new connection_class: client, size: 1, lazy: true
    pool.should be_an_instance_of subject
    pool.connect_timeout.should be_nil
    pool.query_timeout.should be_nil
    pool.connect_timeout = timeout
    pool.query_timeout = timeout
    pool.on_connect = hook
    pool.on_autoreconnect(&hook)
    pool.connect_timeout.should eq timeout
    pool.query_timeout.should eq timeout
    pool.on_connect.should be hook
    pool.on_autoreconnect.should be hook
    pool.max_size.should eq 1
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
    pool.hold
    pool.available.length.should eq 1
    pool.allocated.length.should eq 0
    pool.size.should eq 1
  end

  it "should lazy write attributes to connecting client" do
    pool = nil
    client.should_receive(:new) do |options|
      options.should be_empty
      pool.connect_timeout = timeout
      pool.query_timeout = timeout
      pool.on_connect = hook
      pool.on_autoreconnect(&hook)
      client.allocate.tap do |conn|
        conn.should_receive(:connect_timeout=).with(timeout).once
        conn.should_receive(:query_timeout=).with(timeout).once
        conn.should_receive(:on_autoreconnect=).with(hook).once
        conn.should_receive(:on_connect=).with(hook).once
      end
    end.once
    client.should_receive(:connect_defer) do |options|
      options.should be_empty
      pool.connect_timeout = timeout
      pool.query_timeout = timeout
      pool.on_connect(&hook)
      pool.on_autoreconnect = hook
      deferrable.new.tap do |df|
        df.succeed(client.allocate.tap do |conn|
          conn.should_receive(:connect_timeout=).with(timeout).once
          conn.should_receive(:query_timeout=).with(timeout).once
          conn.should_receive(:on_autoreconnect=).with(hook).once
          conn.should_receive(:on_connect=).with(hook).once
        end)
      end
    end.once
    checkpoint.should_receive(:call).with(:hurray).once

    pool = subject.new connection_class: client, size: 1, lazy: true
    pool.should be_an_instance_of subject
    pool.connect_timeout.should be_nil
    pool.query_timeout.should be_nil
    pool.max_size.should eq 1
    pool.size.should eq 0
    pool.hold
    pool.connect_timeout.should eq timeout
    pool.query_timeout.should eq timeout
    pool.size.should eq 1

    pool = subject.new connection_class: client, size: 1, lazy: true
    pool.should be_an_instance_of subject
    pool.connect_timeout.should be_nil
    pool.query_timeout.should be_nil
    pool.__send__(:hold_deferred, checkpoint) do
      deferrable.new.tap { |df| df.succeed :hurray }
    end
    pool.connect_timeout.should eq timeout
    pool.query_timeout.should eq timeout
    pool.size.should eq 1
  end

  it "should pass missing methods to connection" do
    client.should_receive(:new) do
      client.allocate.tap do |conn|
        conn.should_receive(:foobar).once.and_return(:ok)
      end
    end.once
    pool = subject.new connection_class: client
    pool.should be_an_instance_of subject
    pool.foobar.should be :ok
    pool.respond_to?(:foobar).should be_true
    pool.respond_to?(:crowbar).should be_false
  end

  it "should hold nested commands" do
    ::EM.should_not_receive(:next_tick)
    client.should_receive(:new).with({}).once.and_return(client.allocate)
    pool = subject.new connection_class: client
    pool.should be_an_instance_of subject
    checkpoint.should_receive(:check) do |conn|
      pool.max_size.should eq subject::DEFAULT_SIZE
      pool.available.length.should eq 0
      pool.allocated.length.should eq 1
      pool.size.should eq 1
    end.exactly(3)
    pool.hold do |conn|
      conn.should be_an_instance_of client
      checkpoint.check
      pool.hold do |conn2|
        conn2.should be conn
        checkpoint.check
      end
      checkpoint.check
    end
    pool.max_size.should eq subject::DEFAULT_SIZE
    pool.available.length.should eq 1
    pool.allocated.length.should eq 0
    pool.size.should eq 1
    pool.available.first.should be_an_instance_of client
  end

  it "should hold commands concurrently" do
    ::EM.should_not_receive(:next_tick)
    client.should_receive(:new) { client.allocate }.twice
    pool = subject.new connection_class: client, size: 2
    pool.should be_an_instance_of subject
    pool.max_size.should eq 2
    pool.available.length.should eq 1
    pool.allocated.length.should eq 0
    pool.size.should eq 1
    pool.available.first.should be_an_instance_of client
    checkpoint.should_receive(:check).exactly(8)
    result = []
    pool.hold do |conn1|
      conn1.should be_an_instance_of client
      result << conn1
      pool.max_size.should eq 2
      pool.available.length.should eq 0
      pool.allocated.length.should eq 1
      pool.size.should eq 1
      checkpoint.check
      Fiber.new do
        pool.hold do |conn2|
          conn2.should be_an_instance_of client
          pool.instance_variable_get(:@pending).length.should eq 0
          Fiber.new do
            pool.hold do |conn3|
              result << conn3
              conn3.should be conn2
              pool.available.length.should eq 0
              pool.allocated.length.should eq 2
              pool.size.should eq 2
              checkpoint.check
            end
          end.resume
          pool.instance_variable_get(:@pending).length.should eq 1
          Fiber.new do
            pool.hold do |conn4|
              result << conn4
              result.should eq [conn1, conn2, conn2, conn4]
              conn4.should be conn2
              pool.available.length.should eq 0
              pool.allocated.length.should eq 2
              pool.size.should eq 2
              checkpoint.check
              pool.hold do |conn5|
                conn5.should be conn4
                pool.max_size.should eq 2
                pool.available.length.should eq 0
                pool.allocated.length.should eq 2
                pool.size.should eq 2
                checkpoint.check
              end
              pool.available.length.should eq 0
              pool.allocated.length.should eq 2
              pool.size.should eq 2
              checkpoint.check
            end
          end.resume
          pool.instance_variable_get(:@pending).length.should eq 2
          result << conn2
          conn2.should_not be conn1
          pool.available.length.should eq 0
          pool.allocated.length.should eq 2
          pool.size.should eq 2
          checkpoint.check
        end
        pool.max_size.should eq 2
        pool.available.length.should eq 1
        pool.allocated.length.should eq 1
        pool.size.should eq 2
        checkpoint.check
      end.resume
      pool.instance_variable_get(:@pending).length.should eq 0
    end
    pool.max_size.should eq 2
    pool.available.length.should eq 2
    pool.allocated.length.should eq 0
    pool.size.should eq 2
    checkpoint.check
  end

  it "should hold deferred commands concurrently" do
    ::EM.should_not_receive(:next_tick)
    client.should_not_receive(:new)
    client.should_receive(:connect_defer) do
      deferrable.new.tap {|d| d.succeed client.allocate }
    end.twice
    checkpoint.should_receive(:call).exactly(4)
    checkpoint.should_receive(:check).exactly(9)
    pool = subject.new connection_class: client, size: 2, lazy: true
    pool.should be_an_instance_of subject
    pool.max_size.should eq 2
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
    result = []
    df = pool.__send__(:hold_deferred, checkpoint) do |conn1|
      conn1.should be_an_instance_of client
      pool.available.length.should eq 0
      pool.allocated.length.should eq 1
      checkpoint.check
      df2 = pool.__send__(:hold_deferred, checkpoint) do |conn2|
        conn2.should be_an_instance_of client
        conn2.should_not be conn1
        pool.available.length.should eq 0
        pool.allocated.length.should eq 2
        checkpoint.check
        df3 = pool.__send__(:hold_deferred, checkpoint) do |conn3|
          conn3.should be_an_instance_of client
          conn3.should be conn2
          pool.available.length.should eq 0
          pool.allocated.length.should eq 2
          checkpoint.check
          deferrable.new.tap {|d| d.succeed :result3 }
        end
        df3.should be_an_instance_of deferrable
        df3.should_not be df
        df3.should_not be df2
        df3.callback do |result3|
          result << result3
          result3.should eq :result3
          pool.available.length.should eq 1
          pool.allocated.length.should eq 1
          checkpoint.check
        end
        pool.instance_variable_get(:@pending).length.should eq 1
        deferrable.new.tap {|d| d.succeed :result2 }
      end
      df2.should be_an_instance_of deferrable
      df2.should_not be df
      df2.callback do |result2|
        result << result2
        result2.should eq :result2
        pool.available.length.should eq 1
        pool.allocated.length.should eq 1
        checkpoint.check
      end
      pool.instance_variable_get(:@pending).length.should eq 0
      deferrable.new.tap {|d| d.succeed :result1 }
    end
    df.should be_an_instance_of deferrable
    df.callback do |result1|
      result << result1
      result1.should eq :result1
      pool.available.length.should eq 2
      pool.allocated.length.should eq 0
      checkpoint.check
      df4 = pool.__send__(:hold_deferred, checkpoint) do |conn4|
        conn4.should be_an_instance_of client
        pool.available.length.should eq 1
        pool.allocated.length.should eq 1
        checkpoint.check
        deferrable.new.tap {|d| d.succeed :result4 }
      end
      df4.should be_an_instance_of deferrable
      df4.should_not be df
      df4.callback do |result4|
        result << result4
        result4.should eq :result4
        pool.available.length.should eq 2
        pool.allocated.length.should eq 0
        checkpoint.check
      end
    end
    pool.available.length.should eq 2
    pool.allocated.length.should eq 0
    pool.size.should eq 2
    result.should eq [:result3, :result2, :result1, :result4]
    checkpoint.check
  end

  it "should drop failed connection while connecting" do
    pool = nil
    ::EM.should_not_receive(:next_tick)
    client.should_receive(:new) do
      if pool
        pool.available.length.should eq 0
        pool.allocated.length.should eq 1
        pool.size.should eq 1
      end
      raise PG::Error
    end.twice
    expect do
      subject.new connection_class: client, size: 1
    end.to raise_error PG::Error

    pool = subject.new connection_class: client, size: 1, lazy: true
    pool.should be_an_instance_of subject
    pool.max_size.should eq 1
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
    expect do
      pool.hold
    end.to raise_error PG::Error
    pool.max_size.should eq 1
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
  end

  it "should drop failed connection while connecting asynchronously" do
    pool = nil
    ::EM.should_not_receive(:next_tick)
    client.should_not_receive(:new)
    client.should_receive(:connect_defer) do
      if pool
        pool.available.length.should eq 0
        pool.allocated.length.should eq 1
        pool.size.should eq 1
      end
      deferrable.new.tap {|d| d.fail pgerror }
    end.twice
    checkpoint.should_receive(:call).with(pgerror).exactly(3)
    df = subject.connect_defer connection_class: client, size: 1
    df.should be_an_instance_of deferrable
    df.errback(&checkpoint)

    pool = subject.new connection_class: client, size: 1, lazy: true
    pool.should be_an_instance_of subject
    pool.max_size.should eq 1
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
    df = pool.__send__(:hold_deferred, checkpoint)
    df.should be_an_instance_of deferrable
    df.errback(&checkpoint)
    pool.max_size.should eq 1
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
  end

  it "should drop only failed connection on error" do
    pool = nil
    ::EM.should_not_receive(:next_tick)
    client.should_receive(:new) do
      if pool
        pool.available.length.should eq 0
        pool.allocated.length.should eq 1
        pool.size.should eq 1
      end
      client.allocate
    end.twice
    checkpoint.should_receive(:check).exactly(2)
    pool = subject.new connection_class: client, size: 1
    pool.should be_an_instance_of subject
    pool.max_size.should eq 1
    pool.available.length.should eq 1
    pool.allocated.length.should eq 0
    pool.size.should eq 1
    expect do
      pool.hold do |conn|
        conn.should be_an_instance_of client
        pool.available.length.should eq 0
        pool.allocated.length.should eq 1
        pool.size.should eq 1
        conn.should_receive(:status).once.and_return(PG::CONNECTION_BAD)
        conn.should_receive(:finished?).once.and_return(false)
        conn.should_receive(:finish).once
        checkpoint.check
        raise PG::Error
      end
    end.to raise_error PG::Error
    pool.max_size.should eq 1
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
    expect do
      pool.hold do |conn|
        pool.available.length.should eq 0
        pool.allocated.length.should eq 1
        pool.size.should eq 1
        conn.should be_an_instance_of client
        conn.should_not_receive(:status)
        conn.should_not_receive(:finished?)
        conn.should_not_receive(:finish)
        checkpoint.check
        raise 'foo'
      end
    end.to raise_error RuntimeError, 'foo'
    pool.available.length.should eq 1
    pool.allocated.length.should eq 0
    pool.size.should eq 1
  end

  it "should drop only failed connection on deferred error" do
    pool = nil
    ::EM.should_not_receive(:next_tick)
    client.should_not_receive(:new)
    client.should_receive(:connect_defer) do
      if pool
        pool.available.length.should eq 0
        pool.allocated.length.should eq 1
        pool.size.should eq 1
      end
      deferrable.new.tap {|d| d.succeed client.allocate }
    end.twice
    checkpoint.should_receive(:check).exactly(2)
    checkpoint.should_receive(:call).with(pgerror).exactly(2)
    checkpoint.should_receive(:call).with(fooerror).exactly(2)
    pool = subject.new connection_class: client, size: 1, lazy: true
    pool.should be_an_instance_of subject
    pool.max_size.should eq 1
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
    df = pool.__send__(:hold_deferred, checkpoint) do |conn|
      pool.available.length.should eq 0
      pool.allocated.length.should eq 1
      pool.size.should eq 1
      conn.should be_an_instance_of client
      conn.should_receive(:status).once.and_return(PG::CONNECTION_BAD)
      conn.should_receive(:finished?).once.and_return(false)
      conn.should_receive(:finish).once
      checkpoint.check
      deferrable.new.tap {|d| d.fail pgerror }
    end
    df.should be_an_instance_of deferrable
    df.errback(&checkpoint)
    pool.max_size.should eq 1
    pool.available.length.should eq 0
    pool.allocated.length.should eq 0
    pool.size.should eq 0
    df = pool.__send__(:hold_deferred, checkpoint) do |conn|
      pool.available.length.should eq 0
      pool.allocated.length.should eq 1
      pool.size.should eq 1
      conn.should be_an_instance_of client
      conn.should_not_receive(:status)
      conn.should_not_receive(:finished?)
      conn.should_not_receive(:finish)
      checkpoint.check
      deferrable.new.tap {|d| d.fail fooerror }
    end
    df.should be_an_instance_of deferrable
    df.errback(&checkpoint)
    pool.max_size.should eq 1
    pool.available.length.should eq 1
    pool.allocated.length.should eq 0
    pool.size.should eq 1
  end

  let(:pool_size) { 7 }

  it "pending requests should not starve on failed connections" do
    pool = nil
    client.should_receive(:new) do
      pool.available.length.should eq 0
      pool.size.should be > 0
      pool.size.should be <= pool_size
      pool.size.should eq pool.allocated.length
      sleep_one_tick
      raise PG::ConnectionBad
    end.exactly(100)
    client.should_receive(:connect_defer) do
      pool.available.length.should eq 0
      pool.size.should be > 0
      pool.size.should be <= pool_size
      pool.size.should eq pool.allocated.length
      deferrable.new.tap do |df|
        EM.next_tick { df.fail pgerror }
      end
    end.exactly(100)
    pool = subject.new connection_class: client, lazy: true, size: pool_size
    pool.should be_an_instance_of subject
    pool.max_size.should eq pool_size
    pool.size.should eq 0

    queries = %w[fee bzz bzz fee bzz fee fee bzz fee bzz]*2
    10.times do
      test_queries pool, queries.rotate! do |progress|
        pending = pool.instance_variable_get(:@pending).length
        expected_pending = progress.length - pool.max_size
        pending.should eq [0, expected_pending].max
        pool.available.length.should eq 0
        pool.size.should be >= [pool_size, expected_pending].min
      end
    end

    pool.max_size.should eq pool_size
    pool.size.should eq 0
  end

  it "pending requests should not starve on commands with failing connections" do
    pool = nil
    expected_connections = if pool_size < 20
      200 + pool_size
    elsif pool_size < 40
      220
    elsif pool_size < 60
      180 + pool_size
    else
      240
    end
    checkpoint.should_receive(:check_fiber).exactly(300)
    checkpoint.should_receive(:check_defer).exactly(300)
    checkpoint.should_receive(:connect).exactly(expected_connections)
    client.stub(:new) do
      pool.available.length.should eq 0
      pool.size.should be > 0
      pool.size.should be <= pool_size
      pool.size.should eq pool.allocated.length
      sleep_one_tick
      checkpoint.connect
      create_connection
    end
    pool = subject.new connection_class: client, lazy: true, size: pool_size
    client.stub(:connect_defer) do
      pool.available.length.should eq 0
      pool.size.should be > 0
      pool.size.should be <= pool_size
      pool.size.should eq pool.allocated.length
      checkpoint.connect
      deferrable.new.tap do |df|
        EM.next_tick { df.succeed create_connection }
      end
    end
    pool.should be_an_instance_of subject
    pool.max_size.should eq pool_size
    pool.size.should eq 0

    good_queries          = %w[foo bar bar foo bar foo foo bar foo bar]*2
    disconnecting_queries = %w[fee bzz bzz fee bzz fee fee bzz fee bzz]*2

    10.times do
      test_queries pool, good_queries + disconnecting_queries + good_queries do |progress|
        pending = pool.instance_variable_get(:@pending).length
        expected_pending = progress.length - pool.max_size
        pending.should eq [0, expected_pending].max
        pool.size.should be > 0
      end
      good_queries.rotate!
      disconnecting_queries.rotate!
    end

    pool.max_size.should eq pool_size
    pool.available.length.should eq pool.size
    pool.size.should eq expected_connections - 200
  end

end
