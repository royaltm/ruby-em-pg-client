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
  ensure
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
      it_should_execute("rollback transaction", :query_defer, 'ROLLBACK')
    end

    def it_should_begin
      it_should_execute("begin transaction", :query_defer, 'BEGIN TRANSACTION')
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

  it "should have same internal and external encoding" do
    ensure_em_stop do
      @client.external_encoding.should be @client.internal_encoding
    end
  end

  it_should_begin

  it_should_execute("drop table `foo` if exists",
      :query_defer, 'DROP TABLE IF EXISTS foo')

  it_should_execute("create simple table `foo`",
      :query_defer, 'CREATE TABLE foo (id integer,cdate timestamp with time zone,data varchar)')

end

shared_context 'em-pg common after' do

  if described_class.instance_methods.include? :set_single_row_mode

    it "should get each result in single row mode" do
      @client.get_result_defer do |result|
        result.should be_nil
        @client.send_query('SELECT data, id FROM foo order by id')
        @client.set_single_row_mode
        EM::Iterator.new(@values, 1).map(proc{ |(data, id), iter|
          @client.get_result_defer do |result|
            result.should be_an_instance_of PG::Result
            result.check
            result.result_status.should eq PG::PGRES_SINGLE_TUPLE
            value = result.to_a
            value.should eq [{'data' => data, 'id' => id.to_s}]
            result.clear
            iter.return value
          end.should be_a_kind_of ::EM::DefaultDeferrable
        }, proc{ |results|
          results.length.should eq @values.length
          @client.get_result_defer do |result|
            result.should be_an_instance_of PG::Result
            result.check
            result.result_status.should eq PG::PGRES_TUPLES_OK
            result.to_a.should eq []
            result.clear
            @client.get_result_defer do |result|
              result.should be_nil
              EM.stop
            end.should be_a_kind_of ::EM::DefaultDeferrable
          end.should be_a_kind_of ::EM::DefaultDeferrable
        })
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end

  end

  it_should_execute("create prepared statement",
      :prepare_defer, 'get_foo', 'SELECT * FROM foo order by id')

  it "should describe prepared statement" do
    pg_exec_and_check(@client, :describe_prepared_defer, 'get_foo') do |result|
      result.nfields.should eq 3
      result.fname(0).should eq 'id'      
      result.values.should be_empty
    end
  end

  it "should read foo table with prepared statement" do
    pg_exec_and_check(@client, :exec_prepared_defer, 'get_foo') do |result|
      result.each_with_index do |row, i|
        row['id'].to_i.should == i
        DateTime.parse(row['cdate']).should == @cdates[i]
        row['data'].should == @values[i][0]
      end
    end
  end

  it_should_execute("declare cursor",
    :query_defer, 'DECLARE foobar SCROLL CURSOR FOR SELECT * FROM foo')

  it "should fetch two rows from table" do
    pg_exec_and_check(@client, :query_defer, 'FETCH FORWARD 2 FROM foobar') do |result|
      result.nfields.should eq 3
      result.fname(0).should eq 'id'      
      result.values.length.should eq 2
    end
  end
  
  it "should describe cursor with describe_portal" do
    pg_exec_and_check(@client, :describe_portal_defer, 'foobar') do |result|
      result.nfields.should eq 3
      result.fname(0).should eq 'id'
    end
  end

  it_should_execute("close cursor", :query_defer, 'CLOSE foobar')

  it "should connect to database asynchronously" do
    this = :first
    Encoding.default_internal = Encoding::ISO_8859_1
    described_class.connect_defer do |conn|
      this = :second
      Encoding.default_internal = nil
      conn.should be_an_instance_of described_class
      conn.external_encoding.should_not eq(conn.internal_encoding)
      conn.internal_encoding.should be Encoding::ISO_8859_1
      conn.get_client_encoding.should eq "LATIN1"
      pg_exec_and_check(conn, :query_defer, 'SELECT pg_database_size(current_database());') do |result|
        result[0]['pg_database_size'].to_i.should be > 0
      end
    end.should be_a_kind_of ::EM::DefaultDeferrable
    this.should be :first
  end

  it "should connect without setting incompatible encoding" do
    this = :first
    Encoding.default_internal = Encoding::Emacs_Mule
    described_class.connect_defer do |conn|
      this = :second
      Encoding.default_internal = nil
      conn.should be_an_instance_of described_class
      conn.external_encoding.should be conn.internal_encoding
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
    this.should be :first
  end

  it_should_execute_with_error("raise syntax error in misspelled multiple statement",
      PG::SyntaxError,
      "syntax error",
      :query_defer, 'SELECT * from pg_class; SRELECT CURRENT_TIMESTAMP; SELECT 42 number')

  it_should_rollback

  it "should return only last statement" do
    pg_exec_and_check(@client, :query_defer,
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
        PG::ConnectionBad, "query timeout expired",
        :query_defer, 'SELECT pg_sleep(2)') do
      (Time.now - start_time).should be < 2
      @client.async_command_aborted.should be_true
      @client.status.should be PG::CONNECTION_BAD
      @client.query_timeout = 0
      @client.query_timeout.should eq 0
      @client.async_autoreconnect = false
      pg_exec_and_check_with_error(@client, true,
        PG::ConnectionBad, "previous query expired",
        :query_defer, 'SELECT 1') do
        @client.async_autoreconnect = true
      end
    end
  end

  it "should timeout not expire while executing query with partial results" do
    @client.query_timeout.should eq 0
    @client.query_timeout = 1.1
    @client.query_timeout.should eq 1.1
    start_time = Time.now
    pg_exec_and_check(@client, :query_defer,
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
        PG::ConnectionBad, "query timeout expired",
        :query_defer,
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

  it "should not expire after executing erraneous query" do
    @client.query_timeout.should eq 0
    @client.query_timeout = 0.1
    @client.query_timeout.should eq 0.1
    start_time = Time.now
    pg_exec_and_check_with_error(@client, false,
        PG::SyntaxError, "syntax error",
        :query_defer, 'SELLECT 1') do
      @client.async_command_aborted.should be_false
      ::EM.add_timer(0.11) do
        @client.async_command_aborted.should be_false
        @client.status.should be PG::CONNECTION_OK
        @client.query_timeout = 0
        @client.query_timeout.should eq 0
        EM.stop
      end
    end
  end

  it "should get last result asynchronously" do
    @client.get_last_result_defer do |result|
      result.should be_nil
      @client.send_query('SELECT 1; SELECT 2; SELECT 3')
      @client.get_last_result_defer do |result|
        result.should be_an_instance_of PG::Result
        result.getvalue(0,0).should eq '3'
        result.clear
        @client.get_last_result_defer do |result|
          result.should be_nil
          EM.stop
        end
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should get each result asynchronously" do
    @client.get_result_defer do |result|
      result.should be_nil
      @client.send_query('SELECT 4; SELECT 5; SELECT 6')
      EM::Iterator.new(%w[4 5 6], 1).map(proc{ |value, iter|
        @client.get_result_defer do |result|
          result.should be_an_instance_of PG::Result
          result.check
          result.result_status.should eq PG::PGRES_TUPLES_OK
          result.getvalue(0,0).should eq value
          result.clear
          iter.return value
        end.should be_a_kind_of ::EM::DefaultDeferrable
      }, proc{ |results|
        results.should eq %w[4 5 6]
        @client.get_result_defer do |result|
          result.should be_nil
          EM.stop
        end.should be_a_kind_of ::EM::DefaultDeferrable
      })
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

end
