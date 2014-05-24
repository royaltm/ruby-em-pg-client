$:.unshift "lib"
gem 'eventmachine', '~> 1.0.0'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'eventmachine'
require 'em-synchrony'
require 'em-synchrony/fiber_iterator'
require 'pg/em/connection_pool'

shared_context 'test on_connect' do

  it 'should call prepared statement concurrently with synchrony' do
    results = []
    pool = subject.new(options)
    pool.max_size.should eq concurrency
    pool.size.should eq 1
    start = Time.now
    EM::Synchrony::FiberIterator.new((1..concurrency), concurrency).each do |index|
      pool.exec_prepared(query_name) do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
      end
      pool.query(sleep_query).should be_an_instance_of PG::Result
      results << index
    end
    delta = Time.now - start
    delta.should be_between(sleep_interval, sleep_interval * concurrency / 2)
    results.sort.should eq (1..concurrency).to_a
    pool.size.should eq concurrency
    EM.stop
  end

  it 'should call prepared statement concurrently with deferrable' do
    results = []
    subject.connect_defer(options) do |pool|
      pool.max_size.should eq concurrency
      pool.size.should eq 1
      start = Time.now
      concurrency.times do |index|
        pool.exec_prepared_defer(query_name) do |result|
          result.should be_an_instance_of PG::Result
          result[0]['pg_database_size'].to_i.should be > 0
          pool.query_defer(sleep_query) do |result|
            result.should be_an_instance_of PG::Result
            results << index
            if results.length == concurrency
              delta = Time.now - start
              delta.should be_between(sleep_interval, sleep_interval * concurrency / 2)
              results.sort.should eq (0...concurrency).to_a
              pool.size.should eq concurrency
              EM.stop
            end
          end
        end
      end
    end
  end
end

shared_context 'test on_connect error' do

  it "should fail on_connect with exception after synchrony connect" do
    expect do
      subject.new(options)
    end.to raise_error(on_connect_exception)
    client = subject.new(options.merge(lazy: true))
    client.should be_an_instance_of subject
    expect do
      client.query(sleep_query)
    end.to raise_error(on_connect_exception)
    EM.stop
  end

  it "should fail on_connect with exception after deferrable connect" do
    subject.connect_defer(options) do |ex|
      ex.should be_an_instance_of on_connect_exception
      pool = subject.new(options.merge(lazy: true))
      pool.should be_an_instance_of subject
      pool.query_defer(sleep_query) do |ex|
        ex.should be_an_instance_of on_connect_exception
        EM.stop
      end.should be_an_instance_of PG::EM::FeaturedDeferrable
    end.should be_an_instance_of PG::EM::FeaturedDeferrable
  end

end

describe 'connection pool' do
  subject { PG::EM::ConnectionPool }

  describe 'on_connect' do
    let(:query_name)     { 'get_db_size' }
    let(:query)          { 'SELECT pg_database_size(current_database());' }
    let(:concurrency)    { 10 }
    let(:options)        { {size: concurrency, on_connect: on_connect} }
    let(:sleep_interval) { 0.1 }
    let(:sleep_query)    { "SELECT pg_sleep(#{sleep_interval})"}
    let(:on_connect_exception) { Class.new(StandardError) }
    let(:on_connect)     { proc {} }

    around(:each) do |testcase|
      EM.synchrony do
        begin
          testcase.call
        end
      end
    end

    it 'should setup block as on_connect client option' do
      connect_hook = false
      pool = subject.new { connect_hook = true }
      connect_hook.should be_true
      pool.should be_an_instance_of subject
      pool.on_connect.should be_an_instance_of Proc
      EM.stop
    end

    it 'should prefer on_connect from options' do
      connect_hook = false
      pool = subject.new(options) { connect_hook = true }
      connect_hook.should be_false
      pool.should be_an_instance_of subject
      pool.on_connect.should be on_connect
      EM.stop
    end

    describe 'with deferrable on_connect' do
      let(:on_connect)     { proc {|client, is_async|
        is_async.should be_true
        PG::EM::FeaturedDeferrable.new.tap do |df|
          client.exec_defer(sleep_query).callback do
            df.bind_status client.prepare_defer(query_name, query)
          end.errback { df.fail on_connect_exception }
        end
      } }

      include_context 'test on_connect'
    end

    describe 'with synchrony on_connect' do
      let(:on_connect) { proc {|client, is_async|
        is_async.should be_true
        was_async = false
        EM.next_tick { was_async = true }
        client.exec(sleep_query)
        client.prepare(query_name, query)
        was_async.should be_true
      } }

      include_context 'test on_connect'
    end

    describe 'with error raised in on_connect' do
      let(:on_connect) { proc {|client|
        raise on_connect_exception
      } }

      include_context 'test on_connect error'
    end

    describe 'with on_connect deferrable failure' do
      let(:on_connect) { proc {|client|
        EM::DefaultDeferrable.new.tap {|df| df.fail on_connect_exception.new }
      } }

      include_context 'test on_connect error'
    end
  end

  describe '#transaction' do
    let(:concurrency)    { 2 }
    let(:options)        { {size: concurrency} }
    let(:query)          { 'SELECT pg_database_size(current_database());' }

    around(:each) do |testcase|
      EM.synchrony do
        begin
          @pool = subject.new(options)
          testcase.call
        end
      end
    end

    it 'should lock transaction connection to fiber' do
      over_count = 2
      @pool.transaction do |pg|
        @pool.hold {|c| c.should be pg }
        Fiber.new do
          @pool.size.should eq 1
          @pool.hold {|c| c.should_not be pg }
          @pool.size.should eq 2
          EM.stop if (over_count-=1).zero?
        end.resume
        @pool.hold {|c| c.should be pg }
        @pool.size.should eq 2
      end
      EM.stop if (over_count-=1).zero?
    end

  end
end
