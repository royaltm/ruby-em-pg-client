$:.unshift "lib"
gem 'eventmachine', '~> 1.0.0'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'date'
require 'eventmachine'
require 'pg/em'

$pgserver_cmd_stop = ENV['PG_CTL_STOP_CMD'] || %Q[sudo -i -u postgres pg_ctl -D "#{ENV['PGDATA']}" stop -s -m fast]
$pgserver_cmd_start = ENV['PG_CTL_START_CMD'] || %Q[sudo -i -u postgres pg_ctl -D "#{ENV['PGDATA']}" start -s -w]

DISCONNECTED_ERROR = ENV['PGHOST'].include?('/') ? PG::UnableToSend : PG::ConnectionBad

shared_context 'pg-em common' do
  around(:each) do |testcase|
    EM.run(&testcase)
  end

  after(:all) do
    @client.close
  end
end

describe 'pg-em async connect fail' do
  around(:each) do |testcase|
    begin
      system($pgserver_cmd_stop).should be_true
      testcase.call
    ensure
      system($pgserver_cmd_start).should be_true
    end
  end

  it "should not connect when server is down" do
    error = nil
    EM.run do
      EM.add_timer(1) { EM.stop }
      df = PG::EM::Client.async_connect
      df.callback {|c| c.close }
      df.errback do |err|
        error = err
        EM.stop
      end
    end
    error.should be_an_instance_of PG::ConnectionBad
  end
end

describe 'pg-em default autoreconnect' do
  include_context 'pg-em common'

  it "should not have modified argument Hash" do
    begin
      @options.should eq(async_autoreconnect: true)
    ensure
      EM.stop
    end
  end

  it "should get database size using query" do
    @tested_proc.call
  end

  it "should get database size using query after server restart" do
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @tested_proc.call
  end

  it "should not get database size using query after server shutdown" do
    system($pgserver_cmd_stop).should be_true
    @client.query_defer('SELECT pg_database_size(current_database());') do |ex|
      ex.should be_an_instance_of DISCONNECTED_ERROR
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should get database size using query after server startup" do
    system($pgserver_cmd_start).should be_true
    @tested_proc.call
  end

  it "should fail on invalid query after server restart" do
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.query_defer('SELLECT 1') do |ex|
      ex.should be_an_instance_of PG::SyntaxError
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should fail when in transaction after server restart" do
    @client.query_defer('BEGIN') do |result|
      result.should be_an_instance_of PG::Result
      system($pgserver_cmd_stop).should be_true
      system($pgserver_cmd_start).should be_true
      @client.query_defer('SELECT pg_database_size(current_database());') do |ex|
        ex.should be_an_instance_of DISCONNECTED_ERROR
        @tested_proc.call
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end
  end

  it "should fail to get last result asynchronously after server restart" do
    @client.send_query('SELECT pg_sleep(5); SELECT pg_database_size(current_database());')
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.get_last_result_defer do |ex|
      ex.should be_an_instance_of PG::ConnectionBad
      @client.status.should be PG::CONNECTION_OK
      @client.get_last_result_defer do |result|
        result.should be_nil
        EM.stop
      end
    end.should be_a_kind_of EM::DefaultDeferrable
  end

  it "should fail to get each result asynchronously after server restart" do
    @client.send_query('SELECT pg_sleep(5); SELECT pg_database_size(current_database());')
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.get_result_defer do |result|
      result.should be_an_instance_of PG::Result
      expect do
        result.check
      end.to raise_error PG::Error
      @client.status.should be PG::CONNECTION_OK
      @client.get_result_defer do |ex|
        ex.should be_an_instance_of PG::ConnectionBad
        @client.status.should be PG::CONNECTION_OK
        @client.get_result_defer do |result|
          result.should be_nil
          EM.stop
        end.should be_a_kind_of ::EM::DefaultDeferrable
      end.should be_a_kind_of EM::DefaultDeferrable
    end
  end

  before(:all) do
    @tested_proc = proc do
      @client.query_defer('SELECT pg_database_size(current_database());') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end
    @options = {async_autoreconnect: true}
    @client = PG::EM::Client.new(@options)
    @client.set_notice_processor {|msg| puts "warning from pgsql: #{msg.to_s.chomp.inspect}"}
  end
end

describe 'pg-em autoreconnect with on_autoreconnect' do
  include_context 'pg-em common'

  it "should not have modified argument Hash" do
    begin
      @options.should eq(on_autoreconnect: @on_autoreconnect)
    ensure
      EM.stop
    end
  end

  it "should get database size using prepared statement"do
    @tested_proc.call
  end

  it "should get database size using prepared statement after server restart" do
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @tested_proc.call
  end

  it "should fail on invalid query after server restart" do
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.query_defer('SELLECT 1') do |ex|
      ex.should be_an_instance_of PG::SyntaxError
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should fail when in transaction after server restart" do
    @client.query_defer('BEGIN') do |result|
      result.should be_an_instance_of PG::Result
      system($pgserver_cmd_stop).should be_true
      system($pgserver_cmd_start).should be_true
      @client.query_defer('SELECT pg_database_size(current_database());') do |ex|
        ex.should be_an_instance_of DISCONNECTED_ERROR
        @tested_proc.call
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end
  end

  it "should fail on false from on_autoreconnect after server restart" do
    @client.on_autoreconnect = proc { false }
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.query_defer('SELECT pg_database_size(current_database());') do |ex|
      ex.should be_an_instance_of DISCONNECTED_ERROR
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should complete on true from on_autoreconnect after server restart" do
    @client.on_autoreconnect = proc { true }
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.query_defer('SELECT pg_database_size(current_database());') do |result|
      result.should be_an_instance_of PG::Result
      result[0]['pg_database_size'].to_i.should be > 0
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should fail on query with true from on_autoreconnect after restart" do
    @client.on_autoreconnect = proc { true }
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.query_defer('SELLECT 1') do |ex|
      ex.should be_an_instance_of PG::SyntaxError
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should fail on on_autoreconnect deferrable fail after server restart" do
    @client.on_autoreconnect = proc do
      ::EM::DefaultDeferrable.new.tap {|df| df.fail :boo }
    end
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.query_defer('SELECT 1') do |ex|
      ex.should be :boo
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should fail on raised error in on_autoreconnect after server restart" do
    @client.on_autoreconnect = proc do
      raise TypeError
    end
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.query_defer('SELECT 1') do |ex|
      ex.should be_an_instance_of TypeError
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should fail to get last result asynchronously after server restart" do
    @client.on_autoreconnect = proc { true }
    @client.send_query('SELECT pg_sleep(5); SELECT pg_database_size(current_database());')
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.get_last_result_defer do |ex|
      ex.should be_an_instance_of PG::ConnectionBad
      @client.status.should be PG::CONNECTION_OK
      @client.get_last_result_defer do |result|
        result.should be_nil
        EM.stop
      end
    end.should be_a_kind_of EM::DefaultDeferrable
  end

  it "should fail to get each result asynchronously after server restart" do
    @client.on_autoreconnect = proc {
      EM::DefaultDeferrable.new.tap {|df| df.succeed }
    }
    @client.send_query('SELECT pg_sleep(5); SELECT pg_database_size(current_database());')
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.get_result_defer do |result|
      result.should be_an_instance_of PG::Result
      expect do
        result.check
      end.to raise_error PG::Error
      @client.status.should be PG::CONNECTION_OK
      @client.get_result_defer do |ex|
        ex.should be_an_instance_of PG::ConnectionBad
        @client.status.should be PG::CONNECTION_OK
        @client.get_result_defer do |result|
          result.should be_nil
          EM.stop
        end.should be_a_kind_of ::EM::DefaultDeferrable
      end.should be_a_kind_of EM::DefaultDeferrable
    end
  end

  it "should execute on_connect before on_autoreconnect after server restart" do
    @client.on_connect.should be_nil
    run_on_connect = false
    @client.on_connect = proc do |client, is_async, is_reset|
      client.should be_an_instance_of PG::EM::Client
      is_async.should be_true
      is_reset.should be_true
      client.query_defer('SELECT pg_database_size(current_database());').callback {
        run_on_connect = true
      }
    end
    @client.on_autoreconnect = proc do |client, ex|
      run_on_connect.should be_true
      @on_autoreconnect.call(client, ex)
    end
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @tested_proc.call
  end

  it "should skip on_autoreconnect when on_connect failed after server restart" do
    run_on_connect = false
    run_on_autoreconnect = false
    @client.on_connect = proc do |client, is_async, is_reset|
      client.should be_an_instance_of PG::EM::Client
      is_async.should be_true
      is_reset.should be_true
      client.query_defer('SELLECT 1;').errback {
        run_on_connect = true
      }
    end
    @client.on_autoreconnect = proc do |client, ex|
      run_on_autoreconnect = true
    end
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.exec_prepared_defer('get_db_size') do |ex|
      ex.should be_an_instance_of PG::SyntaxError
      @client.status.should be PG::CONNECTION_OK
      run_on_connect.should be_true
      run_on_autoreconnect.should be_false
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  before(:all) do
    @tested_proc = proc do
      @client.exec_prepared_defer('get_db_size') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end
    @on_autoreconnect = proc do |client, ex|
      df = client.prepare_defer('get_db_size', 'SELECT pg_database_size(current_database());')
      df.should be_a_kind_of ::EM::DefaultDeferrable
      df
    end
    @options = {on_autoreconnect: @on_autoreconnect}
    @client = PG::EM::Client.new(@options)
    @client.set_notice_processor {|msg| puts "warning from pgsql: #{msg.to_s.chomp.inspect}"}
    @client.prepare('get_db_size', 'SELECT pg_database_size(current_database());')
  end
end

describe 'pg-em with autoreconnect disabled' do
  include_context 'pg-em common'

  it "should get database size using query" do
    @tested_proc.call
  end

  it "should not get database size using query after server restart" do
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @client.query_defer('SELECT pg_database_size(current_database());') do |ex|
      ex.should be_an_instance_of DISCONNECTED_ERROR
      @client.status.should be PG::CONNECTION_BAD
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should get database size using query after async manual connection reset" do
    @client.status.should be PG::CONNECTION_BAD
    @client.reset_defer do |conn|
      conn.should be @client
      @client.status.should be PG::CONNECTION_OK
      @tested_proc.call
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should fail to get last result asynchronously after server restart" do
    check_get_last_result = proc do
      @client.get_last_result_defer do |result|
        result.should be_nil
        @client.reset_defer do |conn|
          conn.should be @client
          @client.status.should be PG::CONNECTION_OK
          EM.stop
        end.should be_a_kind_of ::EM::DefaultDeferrable
      end
    end
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    begin
      @client.send_query('SELECT pg_sleep(5); SELECT pg_database_size(current_database());')
    rescue PG::UnableToSend
      @client.status.should be PG::CONNECTION_BAD
      @client.get_last_result_defer do |ex|
        ex.should be_nil
        @client.status.should be PG::CONNECTION_BAD
        check_get_last_result.call
      end.should be_a_kind_of EM::DefaultDeferrable
    else
      @client.get_last_result_defer do |ex|
        ex.should be_an_instance_of PG::ConnectionBad
        @client.status.should be PG::CONNECTION_BAD
        check_get_last_result.call
      end.should be_a_kind_of EM::DefaultDeferrable
    end
  end

  it "should fail to get each result asynchronously after server restart" do
    check_get_result = proc do |expected_class|
      @client.get_result_defer do |result|
        result.should be_an_instance_of expected_class
        @client.status.should be PG::CONNECTION_BAD
        @client.reset_defer do |conn|
          conn.should be @client
          @client.status.should be PG::CONNECTION_OK
          EM.stop
        end.should be_a_kind_of ::EM::DefaultDeferrable
      end.should be_a_kind_of EM::DefaultDeferrable
    end
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    begin
      @client.send_query('SELECT pg_sleep(5); SELECT pg_database_size(current_database());')
    rescue PG::UnableToSend
      @client.get_result_defer do |result|
        result.should be_nil
        @client.status.should be PG::CONNECTION_BAD
        check_get_result.call NilClass
      end
    else
      @client.get_result_defer do |result|
        result.should be_an_instance_of PG::Result
        expect do
          result.check
        end.to raise_error PG::Error
        @client.status.should be PG::CONNECTION_OK
        check_get_result.call PG::ConnectionBad
      end
    end
  end

  before(:all) do
    @tested_proc = proc do
      @client.query_defer('SELECT pg_database_size(current_database());') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end
    @client = PG::EM::Client.new
    @client.set_notice_processor {|msg| puts "warning from pgsql: #{msg.to_s.chomp.inspect}"}
  end
end
