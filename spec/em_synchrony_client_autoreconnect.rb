$:.unshift "lib"
gem 'eventmachine', '~> 1.0.0'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'date'
require 'em-synchrony'
require 'pg/em'

$pgserver_cmd_stop = ENV['PG_CTL_STOP_CMD'] || %Q[sudo -i -u postgres pg_ctl -D "#{ENV['PGDATA']}" stop -s -m fast]
$pgserver_cmd_start = ENV['PG_CTL_START_CMD'] || %Q[sudo -i -u postgres pg_ctl -D "#{ENV['PGDATA']}" start -s -w]

shared_context 'em-synchrony-pg common' do
  around(:each) do |testcase|
    EM.synchrony do
      testcase.call
      EM.stop
    end
  end

  after(:all) do
    @client.close
  end
end

describe 'em-synchrony-pg default autoreconnect' do
  include_context 'em-synchrony-pg common'

  it "should not have modified argument Hash" do
    @options.should eq(async_autoreconnect: true)
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
    expect {
      @tested_proc.call
    }.to raise_error(@client.host.include?('/') ? PG::UnableToSend : PG::ConnectionBad)
  end

  it "should get database size using query after server startup" do
    system($pgserver_cmd_start).should be_true
    @tested_proc.call
  end

  it "should raise an error when in transaction after server restart" do
    expect do
      @client.transaction do
        system($pgserver_cmd_stop).should be_true
        system($pgserver_cmd_start).should be_true
        @tested_proc.call
      end
    end.to raise_error(@client.host.include?('/') ? PG::UnableToSend : PG::ConnectionBad)
    @tested_proc.call
  end

  before(:all) do
    @tested_proc = proc do
      @client.query('SELECT pg_database_size(current_database());') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
      end
    end
    @options = {async_autoreconnect: true}
    @client = PG::EM::Client.new(@options)
    @client.set_notice_processor {|msg| puts "warning from pgsql: #{msg.to_s.chomp.inspect}"}
  end
end

describe 'em-synchrony-pg autoreconnect with on_autoreconnect' do
  include_context 'em-synchrony-pg common'

  it "should not have modified argument Hash" do
    @options.should eq(on_autoreconnect: @on_autoreconnect)
  end

  it "should get database size using prepared statement" do
    @tested_proc.call
  end

  it "should get database size using prepared statement after server restart" do
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    @tested_proc.call
  end

  it "should raise an error when in transaction after server restart" do
    expect do
      @client.transaction do
        system($pgserver_cmd_stop).should be_true
        system($pgserver_cmd_start).should be_true
        @tested_proc.call
      end
    end.to raise_error(@client.host.include?('/') ? PG::UnableToSend : PG::ConnectionBad)
    @tested_proc.call
  end

  before(:all) do
    @tested_proc = proc do
      @client.exec_prepared('get_db_size') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
      end
    end
    @on_autoreconnect = proc do |client, ex|
      client.prepare('get_db_size', 'SELECT pg_database_size(current_database());')
    end
    @options = {on_autoreconnect: @on_autoreconnect}
    @client = PG::EM::Client.new(@options)
    @client.set_notice_processor {|msg| puts "warning from pgsql: #{msg.to_s.chomp.inspect}"}
    @on_autoreconnect.call @client
  end
end

describe 'em-synchrony-pg with autoreconnect disabled' do
  include_context 'em-synchrony-pg common'

  it "should get database size using query" do
    @tested_proc.call
  end

  it "should not get database size using query after server restart" do
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    expect {
      @tested_proc.call
    }.to raise_error(@client.host.include?('/') ? PG::UnableToSend : PG::ConnectionBad)
  end

  it "should get database size using query after manual connection reset" do
    @client.status.should be PG::CONNECTION_BAD
    @client.reset
    @client.status.should be PG::CONNECTION_OK
    @tested_proc.call
  end

  before(:all) do
    @tested_proc = proc do
      @client.query('SELECT pg_database_size(current_database());') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
      end
    end
    @client = PG::EM::Client.new
    @client.set_notice_processor {|msg| puts "warning from pgsql: #{msg.to_s.chomp.inspect}"}
  end
end
