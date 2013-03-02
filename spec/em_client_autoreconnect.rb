$:.unshift "lib"
require 'date'
require 'eventmachine'
require 'pg/em'

$pgserver_cmd_stop = %Q[sudo su - postgres -c 'pg_ctl stop -m fast']
$pgserver_cmd_start = %Q[sudo su - postgres -c 'pg_ctl -l $PGDATA/postgres.log start -w']

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
    error.should be_an_instance_of PG::EM::Errors::ConnectionRefusedError
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
    @client.query('SELECT pg_database_size(current_database());') do |ex|
      ex.should be_an_instance_of PG::EM::Errors::ConnectionRefusedError
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should get database size using query after server startup" do
    system($pgserver_cmd_start).should be_true
    @tested_proc.call
  end

  before(:all) do
    @tested_proc = proc do
      @client.query('SELECT pg_database_size(current_database());') do |result|
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

  before(:all) do
    @tested_proc = proc do
      @client.exec_prepared('get_db_size') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end
    @on_autoreconnect = proc do |client, ex|
      df = client.prepare('get_db_size', 'SELECT pg_database_size(current_database());')
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
    @client.query('SELECT pg_database_size(current_database());') do |ex|
      ex.should be_an_instance_of PG::EM::Errors::QueryError
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should get database size using query after async manual connection reset" do
    @client.status.should be PG::CONNECTION_BAD
    @client.async_reset do |conn|
      conn.should be @client
      @client.status.should be PG::CONNECTION_OK
      @tested_proc.call
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  before(:all) do
    @tested_proc = proc do
      @client.query('SELECT pg_database_size(current_database());') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end
    @client = PG::EM::Client.new
    @client.set_notice_processor {|msg| puts "warning from pgsql: #{msg.to_s.chomp.inspect}"}
  end
end
