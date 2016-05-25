$:.unshift "lib"
gem 'eventmachine', '~> 1.2.0'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'eventmachine'
require 'em-synchrony'
require 'pg/em'
module PGSpecMacros
  def test_blocking
    client = subject.new(options)
    client.should be_an_instance_of subject
    client.on_connect.should be on_connect
    client.exec_prepared(query_name) do |result|
      result.should be_an_instance_of PG::Result
      result[0]['pg_database_size'].to_i.should be > 0
    end
    client.reset
    client.exec_prepared(query_name) do |result|
      result.should be_an_instance_of PG::Result
      result[0]['pg_database_size'].to_i.should be > 0
    end
  end

  def test_blocking_error
    expect do
      subject.new(options)
    end.to raise_error(on_connect_exception)
    client = subject.new
    client.should be_an_instance_of subject
    client.on_connect = on_connect
    expect do
      client.reset
    end.to raise_error(on_connect_exception)
  end
end

RSpec.configure do |config|
  config.include(PGSpecMacros)
end

shared_context 'test on_connect' do

  it "should invoke on_connect after deferrable connect and reset" do
    EM.run do
      subject.connect_defer(options) do |client|
        client.should be_an_instance_of subject
        client.on_connect.should be on_connect
        client.exec_prepared_defer(query_name) do |result|
          result.should be_an_instance_of PG::Result
          result[0]['pg_database_size'].to_i.should be > 0
          client.reset_defer do |client|
            client.should be_an_instance_of subject
            client.exec_prepared_defer(query_name) do |result|
              result.should be_an_instance_of PG::Result
              result[0]['pg_database_size'].to_i.should be > 0
              EM.stop
            end.should be_a_kind_of ::EM::Deferrable
          end.should be_a_kind_of ::EM::Deferrable
        end.should be_a_kind_of ::EM::Deferrable
      end.should be_a_kind_of ::EM::Deferrable
    end
  end

  it "should invoke on_connect after synchrony connect and reset" do
    EM.synchrony do
      test_blocking
      EM.stop
    end
  end

end

shared_context 'test on_connect error' do

  it "should fail on_connect with exception after deferrable connect and reset" do
    EM.run do
      subject.connect_defer(options) do |ex|
        ex.should be_an_instance_of on_connect_exception
        subject.connect_defer do |client|
          client.should be_an_instance_of subject
          client.on_connect = on_connect
          client.reset_defer do |ex|
            ex.should be_an_instance_of on_connect_exception
            EM.stop
          end.should be_a_kind_of ::EM::Deferrable
        end.should be_a_kind_of ::EM::Deferrable
      end.should be_a_kind_of ::EM::Deferrable
    end
  end

  it "should fail on_connect with exception after synchrony connect and reset" do
    EM.synchrony do
      test_blocking_error
      EM.stop
    end
  end
end

shared_context 'test blocking' do
  it "should invoke on_connect after blocking connect and reset" do
    test_blocking
  end
end

shared_context 'test blocking on_connect error' do
  it "should fail on_connect with exception after blocking connect and reset" do
    test_blocking_error
  end
end

describe 'on_connect option' do
  subject          { PG::EM::Client }
  let(:query_name) { 'get_db_size' }
  let(:query)      { 'SELECT pg_database_size(current_database());' }
  let(:options)    { {on_connect: on_connect} }
  let(:sleep_query){ 'SELECT pg_sleep(0.1)'}
  let(:on_connect_exception) { Class.new(StandardError) }


  describe 'with deferrable on_connect' do
    let(:on_connect) { proc {|client, is_async|
      is_async.should be true
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
      is_async.should be true
      was_async = false
      EM.next_tick { was_async = true }
      client.exec(sleep_query)
      client.prepare(query_name, query)
      was_async.should be true
    } }

    include_context 'test on_connect'
  end

  describe 'with blocking on_connect' do
    let(:on_connect) { proc {|client, is_async|
      is_async.should be false
      client.prepare(query_name, query)
    } }

    include_context 'test blocking'
  end

  describe 'with error raised in on_connect' do
    let(:on_connect) { proc {|client|
      raise on_connect_exception
    } }

    include_context 'test on_connect error'
    include_context 'test blocking on_connect error'
  end

  describe 'with on_connect deferrable failure' do
    let(:on_connect) { proc {|client|
      ::EM::DefaultDeferrable.new.tap {|df| df.fail on_connect_exception.new }
    } }

    include_context 'test on_connect error'
  end

end
