module PGSpecMacros
  def self.included(base)
    base.extend(ClassMethods)
  end

  def pg_exec_and_check(client, method, *args, &additional_checks)
    client.__send__(method, *args) do |result|
      result.should be_an_instance_of PG::Result
      additional_checks.call(result) if additional_checks
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  def pg_exec_and_check_with_error(client, stop, err_class, err_message, method, *args, &additional_checks)
    client.__send__(method, *args) do |exception|
      exception.should be_an_instance_of err_class
      exception.to_s.should include err_message if err_message
      additional_checks.call(exception) if additional_checks
      EM.next_tick { EM.stop } if stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  def ensure_em_stop
    yield
    EM.stop
  end

  module ClassMethods
    def it_should_execute(text, method, *args)
      it "should #{text}" do
        pg_exec_and_check(@client, method, *args)
      end
    end

    def it_should_execute_with_error(text, err_class, err_message, method, *args)
      it "should #{text}" do
        pg_exec_and_check_with_error(@client, true, err_class, err_message, method, *args)
      end
    end

    def it_should_rollback
      it_should_execute("rollback transaction", :query, 'ROLLBACK')
    end

    def it_should_begin
      it_should_execute("begin transaction", :query, 'BEGIN TRANSACTION')
    end
  end
end

shared_context 'em-pg common before' do

  around(:each) do |testcase|
    EM.run do
      EM.stop if testcase.call.is_a? Exception
    end
  end

  before(:all) do
    @cdates = []
    @values = Array(('AA'..'ZZ').each_with_index)
    ENV['PGCLIENTENCODING'] = nil
    Encoding.default_internal = nil
    @client = described_class.new
  end

  after(:all) do
    @client.close
  end

  it "should be a client #{PG::VERSION}" do
    ensure_em_stop do
      @client.should be_an_instance_of described_class
    end
  end

  it "should have disabled async_autoreconnect" do
    ensure_em_stop do
      @client.async_autoreconnect.should be_false
    end
  end
  
  it "should enable async_autoreconnect" do
    ensure_em_stop do
      @client.async_autoreconnect = true
      @client.async_autoreconnect.should be_true
    end
  end

  it "should set async_autoreconnect according to on_autoreconnect" do
    ensure_em_stop do
      on_autoreconnect = proc {|c, e| false }
      async_args = described_class.parse_async_args([])
      async_args.should be_an_instance_of Hash
      async_args[:@on_autoreconnect].should be_nil
      async_args[:@async_autoreconnect].should be_false
      args = [on_autoreconnect: on_autoreconnect]
      async_args = described_class.parse_async_args(args)
      args.should eq [{}]
      async_args.should be_an_instance_of Hash
      async_args[:@on_autoreconnect].should be on_autoreconnect
      async_args[:@async_autoreconnect].should be_true
      args = [async_autoreconnect: false,
        on_autoreconnect: on_autoreconnect]
      async_args = described_class.parse_async_args(args)
      args.should eq [{}]
      async_args.should be_an_instance_of Hash
      async_args[:@on_autoreconnect].should be on_autoreconnect
      async_args[:@async_autoreconnect].should be_false
      args = [on_autoreconnect: on_autoreconnect,
        async_autoreconnect: false]
      async_args = described_class.parse_async_args(args)
      args.should eq [{}]
      async_args.should be_an_instance_of Hash
      async_args[:@on_autoreconnect].should be on_autoreconnect
      async_args[:@async_autoreconnect].should be_false
    end
  end

  it "should have same internal and external encoding" do
    ensure_em_stop do
      @client.external_encoding.should be @client.internal_encoding
    end
  end

  it_should_begin

  it_should_execute("drop table `foo` if exists",
      :query, 'DROP TABLE IF EXISTS foo')

  it_should_execute("create simple table `foo`",
      :query, 'CREATE TABLE foo (id integer,cdate timestamp with time zone,data varchar)')

end

shared_context 'em-pg common after' do

  it_should_execute("create prepared statement",
      :prepare, 'get_foo', 'SELECT * FROM foo order by id')

  it "should describe prepared statement" do
    pg_exec_and_check(@client, :describe_prepared, 'get_foo') do |result|
      result.nfields.should eq 3
      result.fname(0).should eq 'id'      
      result.values.should be_empty
    end
  end

  it "should read foo table with prepared statement" do
    pg_exec_and_check(@client, :exec_prepared, 'get_foo') do |result|
      result.each_with_index do |row, i|
        row['id'].to_i.should == i
        DateTime.parse(row['cdate']).should == @cdates[i]
        row['data'].should == @values[i][0]
      end
    end
  end

  it_should_execute("declare cursor",
    :query, 'DECLARE foobar SCROLL CURSOR FOR SELECT * FROM foo')

  it "should fetch two rows from table" do
    pg_exec_and_check(@client, :query, 'FETCH FORWARD 2 FROM foobar') do |result|
      result.nfields.should eq 3
      result.fname(0).should eq 'id'      
      result.values.length.should eq 2
    end
  end
  
  it "should describe cursor with describe_portal" do
    pg_exec_and_check(@client, :describe_portal, 'foobar') do |result|
      result.nfields.should eq 3
      result.fname(0).should eq 'id'
    end
  end

  it_should_execute("close cursor", :query, 'CLOSE foobar')

  it "should connect to database asynchronously" do
    this = :first
    Encoding.default_internal = Encoding::ISO_8859_1
    described_class.async_connect do |conn|
      this = :second
      Encoding.default_internal = nil
      conn.should be_an_instance_of described_class
      conn.external_encoding.should_not eq(conn.internal_encoding)
      conn.internal_encoding.should be Encoding::ISO_8859_1
      conn.get_client_encoding.should eq "LATIN1"
      pg_exec_and_check(conn, :query, 'SELECT pg_database_size(current_database());') do |result|
        result[0]['pg_database_size'].to_i.should be > 0
      end
    end.should be_a_kind_of ::EM::DefaultDeferrable
    this.should be :first
  end

  it "should connect without setting incompatible encoding" do
    this = :first
    Encoding.default_internal = Encoding::Emacs_Mule
    described_class.async_connect do |conn|
      this = :second
      Encoding.default_internal = nil
      conn.should be_an_instance_of described_class
      conn.external_encoding.should be conn.internal_encoding
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
    this.should be :first
  end

  it_should_execute_with_error("raise syntax error in misspelled multiple statement",
      described_class::QueryError,
      "syntax error",
      :query, 'SELECT * from pg_class; SRELECT CURRENT_TIMESTAMP; SELECT 42 number')

  it_should_rollback

  it "should return only last statement" do
    pg_exec_and_check(@client, :query,
      'SELECT * from pg_class; SELECT CURRENT_TIMESTAMP; SELECT 42 number') do |result|
      result[0]['number'].should eq "42"
    end
  end

  it "should timeout expire while executing query" do
    @client.query_timeout.should eq 0
    @client.query_timeout = 1.5
    @client.query_timeout.should eq 1.5
    start_time = Time.now
    pg_exec_and_check_with_error(@client, false,
        described_class::QueryTimeoutError, "query timeout expired",
        :query, 'SELECT pg_sleep(2)') do
      (Time.now - start_time).should be < 2
      @client.async_command_aborted.should be_true
      @client.status.should be PG::CONNECTION_BAD
      @client.query_timeout = 0
      @client.query_timeout.should eq 0
      @client.async_autoreconnect = false
      pg_exec_and_check_with_error(@client, true,
        described_class::QueryBadStateError, nil,
        :query, 'SELECT 1') do
        @client.async_autoreconnect = true
      end
    end
  end

  it "should timeout not expire while executing query with partial results" do
    @client.query_timeout.should eq 0
    @client.query_timeout = 1.1
    @client.query_timeout.should eq 1.1
    start_time = Time.now
    pg_exec_and_check(@client, :query,
        'SELECT * from pg_class;' +
        'SELECT pg_sleep(1);' +
        'SELECT * from pg_class;' + 
        'SELECT pg_sleep(1);' +
        'SELECT 42 number') do |result|
      (Time.now - start_time).should be > 2
      result[0]['number'].should eq "42"
      @client.query_timeout = 0
      @client.query_timeout.should eq 0
      @client.async_command_aborted.should be_false
      @client.status.should be PG::CONNECTION_OK
    end
  end

  it "should timeout expire while executing query with partial results" do
    @client.query_timeout.should eq 0
    @client.query_timeout = 1.1
    @client.query_timeout.should eq 1.1
    start_time = Time.now
    pg_exec_and_check_with_error(@client, true,
        described_class::QueryTimeoutError, "query timeout expired",
        :query,
        'SELECT * from pg_class;' +
        'SELECT pg_sleep(1);' +
        'SELECT * from pg_class;' + 
        'SELECT pg_sleep(2);' +
        'SELECT 42 number') do
      (Time.now - start_time).should be > 2
      @client.async_command_aborted.should be_true
      @client.status.should be PG::CONNECTION_BAD
      @client.query_timeout = 0
      @client.query_timeout.should eq 0
    end
  end

  it "should clear connection with blocking reset" do
    ensure_em_stop do
      @client.async_command_aborted.should be_true
      @client.status.should be PG::CONNECTION_BAD
      @client.reset
      @client.async_command_aborted.should be_false
      @client.status.should be PG::CONNECTION_OK
    end
  end
end
