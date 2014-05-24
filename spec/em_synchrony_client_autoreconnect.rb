$:.unshift "lib"
gem 'eventmachine', '~> 1.0.0'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'date'
require 'em-synchrony'
require 'pg/em'

$pgserver_cmd_stop = ENV['PG_CTL_STOP_CMD'] || %Q[sudo -i -u postgres pg_ctl -D "#{ENV['PGDATA']}" stop -s -m fast]
$pgserver_cmd_start = ENV['PG_CTL_START_CMD'] || %Q[sudo -i -u postgres pg_ctl -D "#{ENV['PGDATA']}" start -s -w]

DISCONNECTED_ERROR = ENV['PGHOST'].include?('/') ? PG::UnableToSend : PG::ConnectionBad

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
    }.to raise_error DISCONNECTED_ERROR
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
    end.to raise_error DISCONNECTED_ERROR
    @tested_proc.call
  end

  it "should fail to get last result asynchronously after server restart" do
    @client.send_query('SELECT pg_sleep(50); SELECT pg_database_size(current_database());')
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    expect do
      @client.get_last_result
    end.to raise_error PG::ConnectionBad
    @client.status.should be PG::CONNECTION_OK
    @client.get_last_result.should be_nil
    EM.stop
  end

  it "should fail to get each result asynchronously after server restart" do
    @client.send_query('SELECT pg_sleep(50); SELECT pg_database_size(current_database());')
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    result = @client.get_result
    result.should be_an_instance_of PG::Result
    expect do
      result.check
    end.to raise_error PG::Error
    @client.status.should be PG::CONNECTION_OK
    expect do
      @client.get_result
    end.to raise_error PG::ConnectionBad
    @client.status.should be PG::CONNECTION_OK
    @client.get_result.should be_nil
    EM.stop
  end

  it "should fail wait_for_notify while server restarts" do
    @client.status.should be PG::CONNECTION_OK
    f = Fiber.current
    notify_flag = false
    Fiber.new do
      expect {
        @client.wait_for_notify do
          raise "This block should not be called"
        end
      }.to raise_error(PG::ConnectionBad)
      @client.status.should be PG::CONNECTION_OK
      Fiber.new do
        @client.wait_for_notify do |name,|
          name.should eq 'em_synchrony_client_autoreconnect'
          notify_flag = true
        end.should eq 'em_synchrony_client_autoreconnect'
        @client.query('UNLISTEN *').should be_an_instance_of PG::Result
        f.resume
      end.resume
      @client.query('LISTEN em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
      @client.query('NOTIFY em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
    end.resume
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    Fiber.yield
    notify_flag.should be_true
  end

  it "should fail wait_for_notify and finish slow query while server restarts" do
    @client.status.should be PG::CONNECTION_OK
    f = Fiber.current
    notify_flag = false
    query_flag = false
    start_time = Time.now
    Fiber.new do
      result = @client.query('SELECT pg_sleep(2); SELECT 42')
      result.should be_an_instance_of PG::Result
      result.getvalue(0,0).to_i.should eq 42
      (Time.now - start_time).should be > 2
      query_flag = true
    end.resume
    Fiber.new do
      expect {
        @client.wait_for_notify do
          raise "This block should not be called"
        end
      }.to raise_error(PG::ConnectionBad)
      query_flag.should be_true
      @client.status.should be PG::CONNECTION_OK
      Fiber.new do
        @client.wait_for_notify do |name,|
          name.should eq 'em_synchrony_client_autoreconnect'
          notify_flag = true
        end.should eq 'em_synchrony_client_autoreconnect'
        @client.query('UNLISTEN *').should be_an_instance_of PG::Result
        f.resume
      end.resume
      @client.query('LISTEN em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
      @client.query('NOTIFY em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
    end.resume
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    Fiber.yield
    notify_flag.should be_true
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
    end.to raise_error DISCONNECTED_ERROR
    @tested_proc.call
  end

  it "should fail to get last result asynchronously after server restart" do
    @client.on_autoreconnect = proc {
      EM::DefaultDeferrable.new.tap {|df| df.succeed }
    }
    @client.send_query('SELECT pg_sleep(50); SELECT pg_database_size(current_database());')
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    expect do
      @client.get_last_result
    end.to raise_error PG::ConnectionBad
    @client.status.should be PG::CONNECTION_OK
    @client.get_last_result.should be_nil
    EM.stop
  end

  it "should fail to get each result asynchronously after server restart" do
    @client.on_autoreconnect = proc { true }
    @client.send_query('SELECT pg_sleep(50); SELECT pg_database_size(current_database());')
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    result = @client.get_result
    result.should be_an_instance_of PG::Result
    expect do
      result.check
    end.to raise_error PG::Error
    @client.status.should be PG::CONNECTION_OK
    expect do
      @client.get_result
    end.to raise_error PG::ConnectionBad
    @client.status.should be PG::CONNECTION_OK
    @client.get_result.should be_nil
    EM.stop
  end

  it "should fail wait_for_notify while server restarts" do
    @client.status.should be PG::CONNECTION_OK
    @client.on_autoreconnect(&@on_autoreconnect)
    f = Fiber.current
    notify_flag = false
    Fiber.new do
      expect {
        @client.wait_for_notify do
          raise "This block should not be called"
        end
      }.to raise_error(PG::ConnectionBad)
      @client.status.should be PG::CONNECTION_OK
      Fiber.new do
        @client.wait_for_notify do |name,|
          name.should eq 'em_synchrony_client_autoreconnect'
          notify_flag = true
        end.should eq 'em_synchrony_client_autoreconnect'
        @client.query('UNLISTEN *').should be_an_instance_of PG::Result
        f.resume
      end.resume
      @client.query('LISTEN em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
      @client.query('NOTIFY em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
    end.resume
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    Fiber.yield
    notify_flag.should be_true
    @tested_proc.call
  end

  it "should fail wait_for_notify and finish slow query while server restarts" do
    @client.status.should be PG::CONNECTION_OK
    @client.on_autoreconnect = @on_autoreconnect
    f = Fiber.current
    notify_flag = false
    query_flag = false
    start_time = Time.now
    Fiber.new do
      result = @client.query('SELECT pg_sleep(2); SELECT 42')
      result.should be_an_instance_of PG::Result
      result.getvalue(0,0).to_i.should eq 42
      (Time.now - start_time).should be > 2
      query_flag = true
    end.resume
    Fiber.new do
      expect {
        @client.wait_for_notify do
          raise "This block should not be called"
        end
      }.to raise_error(PG::ConnectionBad)
      query_flag.should be_true
      @client.status.should be PG::CONNECTION_OK
      Fiber.new do
        @client.wait_for_notify do |name,|
          name.should eq 'em_synchrony_client_autoreconnect'
          notify_flag = true
        end.should eq 'em_synchrony_client_autoreconnect'
        @client.query('UNLISTEN *').should be_an_instance_of PG::Result
        f.resume
      end.resume
      @client.query('LISTEN em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
      @client.query('NOTIFY em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
    end.resume
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    Fiber.yield
    notify_flag.should be_true
    @tested_proc.call
  end

  it "should execute on_connect before on_autoreconnect after server restart" do
    @client.on_connect.should be_nil
    run_on_connect = false
    @client.on_connect = proc do |client, is_async, is_reset|
      client.should be_an_instance_of PG::EM::Client
      is_async.should be_true
      is_reset.should be_true
      client.query('SELECT pg_database_size(current_database());') {
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
    EM.stop
  end

  it "should skip on_autoreconnect when on_connect failed after server restart" do
    run_on_connect = false
    run_on_autoreconnect = false
    @client.on_connect = proc do |client, is_async, is_reset|
      client.should be_an_instance_of PG::EM::Client
      is_async.should be_true
      is_reset.should be_true
      begin
        client.query('SELLECT 1;')
      ensure
        run_on_connect = true
      end
    end
    @client.on_autoreconnect = proc do |client, ex|
      run_on_autoreconnect = true
    end
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    expect do
      @client.exec_prepared('get_db_size')
    end.to raise_error PG::SyntaxError
    @client.status.should be PG::CONNECTION_OK
    run_on_connect.should be_true
    run_on_autoreconnect.should be_false
    EM.stop
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
    }.to raise_error DISCONNECTED_ERROR
  end

  it "should get database size using query after manual connection reset" do
    @client.status.should be PG::CONNECTION_BAD
    @client.reset
    @client.status.should be PG::CONNECTION_OK
    @tested_proc.call
  end

  it "should fail to get last result asynchronously after server restart" do
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    begin
      @client.send_query('SELECT pg_sleep(50); SELECT pg_database_size(current_database());')
    rescue PG::UnableToSend
      @client.status.should be PG::CONNECTION_BAD
      @client.get_last_result.should be_nil
    else
      expect do
        @client.get_last_result
      end.to raise_error PG::ConnectionBad
    end
    @client.status.should be PG::CONNECTION_BAD
    @client.get_last_result.should be_nil
    @client.reset
    @client.status.should be PG::CONNECTION_OK
    @client.get_last_result.should be_nil
    EM.stop
  end

  it "should fail to get each result asynchronously after server restart" do
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    begin
      @client.send_query('SELECT pg_sleep(50); SELECT pg_database_size(current_database());')
    rescue PG::UnableToSend
      @client.status.should be PG::CONNECTION_BAD
      @client.get_result.should be_nil
    else
      result = @client.get_result
      result.should be_an_instance_of PG::Result
      expect do
        result.check
      end.to raise_error PG::Error
      @client.status.should be PG::CONNECTION_OK
      expect do
        @client.get_result
      end.to raise_error PG::ConnectionBad
    end
    @client.status.should be PG::CONNECTION_BAD
    @client.get_result.should be_nil
    @client.status.should be PG::CONNECTION_BAD
    @client.reset
    @client.status.should be PG::CONNECTION_OK
    @client.get_result.should be_nil
    EM.stop
  end

  it "should fail wait_for_notify while server restarts" do
    @client.status.should be PG::CONNECTION_OK
    f = Fiber.current
    notify_flag = false
    Fiber.new do
      expect {
        @client.wait_for_notify do
          raise "This block should not be called"
        end
      }.to raise_error(PG::ConnectionBad)
      @client.status.should be PG::CONNECTION_BAD
      expect {
        @client.wait_for_notify do
          raise "This block should not be called"
        end
      }.to raise_error(PG::ConnectionBad)
      @client.status.should be PG::CONNECTION_BAD
      @client.reset
      @client.status.should be PG::CONNECTION_OK
      Fiber.new do
        @client.wait_for_notify do |name,|
          name.should eq 'em_synchrony_client_autoreconnect'
          notify_flag = true
        end.should eq 'em_synchrony_client_autoreconnect'
        @client.query('UNLISTEN *').should be_an_instance_of PG::Result
        f.resume
      end.resume
      @client.query('LISTEN em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
      @client.query('NOTIFY em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
    end.resume
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    Fiber.yield
    notify_flag.should be_true
  end

  it "should fail both wait_for_notify and slow query while server restarts" do
    @client.status.should be PG::CONNECTION_OK
    f = Fiber.current
    notify_flag = false
    query_flag = false
    Fiber.new do
      expect {
        @client.query('SELECT pg_sleep(2); SELECT 42')
      }.to raise_error(PG::ConnectionBad)
      query_flag = true
    end.resume
    Fiber.new do
      expect {
        @client.wait_for_notify do
          raise "This block should not be called"
        end
      }.to raise_error(PG::ConnectionBad)
      query_flag.should be_true
      @client.status.should be PG::CONNECTION_BAD
      expect {
        @client.wait_for_notify do
          raise "This block should not be called"
        end
      }.to raise_error(PG::ConnectionBad)
      @client.status.should be PG::CONNECTION_BAD
      expect {
        @client.query('SELECT 1')
      }.to raise_error(PG::UnableToSend)
      @client.reset
      @client.status.should be PG::CONNECTION_OK
      Fiber.new do
        @client.wait_for_notify do |name,|
          name.should eq 'em_synchrony_client_autoreconnect'
          notify_flag = true
        end.should eq 'em_synchrony_client_autoreconnect'
        @client.query('UNLISTEN *').should be_an_instance_of PG::Result
        f.resume
      end.resume
      @client.query('LISTEN em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
      @client.query('NOTIFY em_synchrony_client_autoreconnect').should be_an_instance_of PG::Result
    end.resume
    system($pgserver_cmd_stop).should be_true
    system($pgserver_cmd_start).should be_true
    Fiber.yield
    notify_flag.should be_true
  end

  it "should fail wait_for_notify when server was shutdown" do
    @client.status.should be PG::CONNECTION_OK
    @client.wait_for_notify(0.1) do
      raise "This block should not be called"
    end.should be_nil
    system($pgserver_cmd_stop).should be_true
    expect {
      @client.wait_for_notify do
        raise "This block should not be called"
      end
    }.to raise_error(PG::ConnectionBad)
    @client.status.should be PG::CONNECTION_BAD
    expect {
      @client.wait_for_notify do
        raise "This block should not be called"
      end
    }.to raise_error(PG::ConnectionBad)
    @client.status.should be PG::CONNECTION_BAD
    system($pgserver_cmd_start).should be_true
    @client.status.should be PG::CONNECTION_BAD
    @client.reset
    @client.status.should be PG::CONNECTION_OK
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
