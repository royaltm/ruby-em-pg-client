$:.unshift "lib"
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'date'
require 'em-synchrony'
require 'em-synchrony/pg'

describe PG::EM::Client do

  it "should be client #{PG::VERSION}" do
    @client.should be_an_instance_of described_class
  end

  it "should have disabled async_autoreconnect" do
    @client.async_autoreconnect.should be_false
  end
  
  it "should enable async_autoreconnect" do
    @client.async_autoreconnect = true
    @client.async_autoreconnect.should be_true
  end

  it "should have same internal and external encoding" do
    @client.external_encoding.should be @client.internal_encoding
  end

  it "should begin transaction" do
    @client.query('BEGIN TRANSACTION').should be_an_instance_of PG::Result
  end

  it "should drop table `foo` if exists" do
    @client.query(
      'DROP TABLE IF EXISTS foo'
    ).should be_an_instance_of PG::Result
  end
  
  it "should create simple table `foo`" do
    @client.query(
      'CREATE TABLE foo (id integer,cdate timestamp with time zone,data varchar)'
    ).should be_an_instance_of PG::Result
  end

  it "should populate foo with some data " do
    results = @values.map do |(data, id)|
      @client.exec_params('INSERT INTO foo (id,cdate,data) VALUES($1,$2,$3) returning cdate', [id, DateTime.now, data]) do |result|
        result.should be_an_instance_of PG::Result
        DateTime.parse(result[0]['cdate'])
      end
    end
    @cdates.replace results
    results.length.should == @values.length
    results.each {|r| r.should be_an_instance_of DateTime }
  end

  it "should create prepared statement" do
    @client.prepare('get_foo', 
      'SELECT * FROM foo order by id'
    ).should be_an_instance_of PG::Result
  end

  it "should describe prepared statement" do
    @client.describe_prepared('get_foo') do |result|
      result.should be_an_instance_of PG::Result
      result.nfields.should eq 3
      result.fname(0).should eq 'id'      
      result.values.should be_empty
    end
  end

  it "should read foo table with prepared statement" do
    ret = @client.exec_prepared('get_foo') do |result|
      result.should be_an_instance_of PG::Result
      result.each_with_index do |row, i|
        row['id'].to_i.should == i
        DateTime.parse(row['cdate']).should == @cdates[i]
        row['data'].should == @values[i][0]
      end
      result
    end
    ret.should be_an_instance_of PG::Result
    expect { ret.fields }.to raise_error(PG::Error, /result has been cleared/)
  end

  it "should declare cursor" do
    @client.query(
      'DECLARE foobar SCROLL CURSOR FOR SELECT * FROM foo'
    ).should be_an_instance_of PG::Result
  end

  it "should fetch two rows from table" do
    ret = @client.query('FETCH FORWARD 2 FROM foobar') do |result|
      result.should be_an_instance_of PG::Result
      result.nfields.should eq 3
      result.fname(0).should eq 'id'      
      result.values.length.should eq 2
      result
    end
    ret.should be_an_instance_of PG::Result
    expect { ret.fields }.to raise_error(PG::Error, /result has been cleared/)
  end
  
  it "should describe cursor with describe_portal" do
    @client.describe_portal('foobar') do |result|
      result.should be_an_instance_of PG::Result
      result.nfields.should eq 3
      result.fname(0).should eq 'id'
    end
  end

  it "should close cursor" do
    @client.query(
      'CLOSE foobar'
    ).should be_an_instance_of PG::Result
  end

  it "should connect to database asynchronously" do
    this = :first
    Encoding.default_internal = Encoding::ISO_8859_1
    f = Fiber.current
    Fiber.new do
      begin
        result = described_class.new do |conn|
          this = :second
          Encoding.default_internal = nil
          conn.should be_an_instance_of described_class
          conn.external_encoding.should_not eq(conn.internal_encoding)
          conn.internal_encoding.should be Encoding::ISO_8859_1
          conn.get_client_encoding.should eq "LATIN1"
          conn.query('SELECT pg_database_size(current_database());') do |result|
            result.should be_an_instance_of PG::Result
            result[0]['pg_database_size'].to_i.should be > 0
          end
          conn
        end
        result.should be_an_instance_of described_class
        result.finished?.should be_true
      ensure
        f.resume
      end
    end.resume
    this.should be :first
    Fiber.yield
    this.should be :second
  end

  it "should connect without setting incompatible encoding" do
    this = :first
    Encoding.default_internal = Encoding::Emacs_Mule
    f = Fiber.current
    Fiber.new do
      begin
        described_class.new do |conn|
          this = :second
          Encoding.default_internal = nil
          conn.should be_an_instance_of described_class
          conn.external_encoding.should be conn.internal_encoding
        end
      ensure
        f.resume
      end
    end.resume
    this.should be :first
    Fiber.yield
    this.should be :second
  end

  it "should raise syntax error in misspelled multiple statement" do
    expect {
      @client.query('SELECT * from pg_class; SRELECT CURRENT_TIMESTAMP; SELECT 42 number')
    }.to raise_error(described_class::QueryError, /syntax error/)
  end
  
  it "should rollback transaction" do
    @client.query('ROLLBACK') do |result|
      result.should be_an_instance_of PG::Result
    end
  end

  it "should return only last statement" do
    @client.query('SELECT * from pg_class; SELECT CURRENT_TIMESTAMP; SELECT 42 number') do |result|
      result.should be_an_instance_of PG::Result
      result[0]['number'].should eq "42"
    end
  end

  it "should timeout expire while executing query" do
    @client.query_timeout.should eq 0
    @client.query_timeout = 1.5
    @client.query_timeout.should eq 1.5
    start_time = Time.now
    expect {
      @client.query('SELECT pg_sleep(2)')
    }.to raise_error(described_class::QueryTimeoutError, /query timeout expired/)
    (Time.now - start_time).should be < 2
    @client.query_timeout = 0
    @client.query_timeout.should eq 0
    @client.async_command_aborted.should be_true
    @client.status.should be PG::CONNECTION_BAD
    @client.async_autoreconnect = false
    expect {
      @client.query('SELECT 1')
    }.to raise_error(described_class::QueryBadStateError)
    @client.async_autoreconnect = true
  end

  it "should timeout not expire while executing query with partial results" do
    @client.query_timeout.should eq 0
    @client.query_timeout = 1.1
    @client.query_timeout.should eq 1.1
    start_time = Time.now
    @client.query(
        'SELECT * from pg_class;' +
        'SELECT pg_sleep(1);' +
        'SELECT * from pg_class;' + 
        'SELECT pg_sleep(1);' +
        'SELECT 42 number') do |result|
      (Time.now - start_time).should be > 2
      result.should be_an_instance_of PG::Result
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
    expect {
      @client.query(
          'SELECT * from pg_class;' +
          'SELECT pg_sleep(1);' +
          'SELECT * from pg_class;' + 
          'SELECT pg_sleep(2);' +
          'SELECT 42 number')
    }.to raise_error(described_class::QueryTimeoutError, /query timeout expired/)
    (Time.now - start_time).should be > 2
    @client.async_command_aborted.should be_true
    @client.status.should be PG::CONNECTION_BAD
    @client.query_timeout = 0
    @client.query_timeout.should eq 0
  end

  it "should clear connection with blocking reset" do
    @after_em_stop = proc do
      @client.async_command_aborted.should be_true
      @client.status.should be PG::CONNECTION_BAD
      @client.reset
      @client.async_command_aborted.should be_false
      @client.status.should be PG::CONNECTION_OK
    end
  end

  describe 'PG::EM::Client#transaction' do

    it "should commit transaction and return whatever block yields" do
      @client.transaction_status.should be PG::PQTRANS_IDLE
      @client.transaction do |pg|
        pg.should be @client
        @client.transaction_status.should be PG::PQTRANS_INTRANS
        @client.instance_variable_get(:@client_tran_count).should eq 1
        @client.query(
          'DROP TABLE IF EXISTS bar'
        ).should be_an_instance_of PG::Result
        @client.query(
          'CREATE TABLE bar (key integer, value varchar)'
        ).should be_an_instance_of PG::Result
        @client.query("INSERT INTO bar (key,value) VALUES(42,'xyz') returning value") do |result|
          result.should be_an_instance_of PG::Result
          result[0]['value']
        end
      end.should eq 'xyz'
      @client.query('SELECT * FROM bar') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['key'].should eq '42'
      end
      @client.transaction_status.should be PG::PQTRANS_IDLE
      @client.instance_variable_get(:@client_tran_count).should eq 0
    end

    it "should rollback transaction on error and raise that error" do
      @client.transaction_status.should be PG::PQTRANS_IDLE
      expect do
        @client.transaction do |pg|
          pg.should be @client
          @client.transaction_status.should be PG::PQTRANS_INTRANS
          @client.instance_variable_get(:@client_tran_count).should eq 1
          @client.query(
            "INSERT INTO bar (key,value) VALUES(11,'abc')"
          ).should be_an_instance_of PG::Result
          @client.query('SELECT * FROM bar ORDER BY key') do |result|
            result.should be_an_instance_of PG::Result
            result[0]['key'].should eq '11'
          end
          @client.query('SELECT count(*) AS count FROM bar') do |result|
            result.should be_an_instance_of PG::Result
            result[0]['count'].should eq '2'
          end
          raise "rollback"
        end
      end.to raise_error(RuntimeError, /rollback/)
      @client.query('SELECT count(*) AS count FROM bar') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['count'].should eq '1'
      end
      @client.transaction_status.should be PG::PQTRANS_IDLE
      @client.instance_variable_get(:@client_tran_count).should eq 0
    end

    it "should allow nesting transaction and return whatever innermost block yields" do
      @client.transaction_status.should be PG::PQTRANS_IDLE
      @client.transaction do |pg|
        pg.should be @client
        @client.transaction_status.should be PG::PQTRANS_INTRANS
        @client.instance_variable_get(:@client_tran_count).should eq 1
        @client.query(
          "INSERT INTO bar (key,value) VALUES(100,'hundred') returning value"
        ).should be_an_instance_of PG::Result
        @client.transaction do |pg|
          pg.should be @client
          @client.transaction_status.should be PG::PQTRANS_INTRANS
          @client.instance_variable_get(:@client_tran_count).should eq 2
          @client.query(
            "INSERT INTO bar (key,value) VALUES(1000,'thousand') returning value"
          ).should be_an_instance_of PG::Result
          @client.transaction do |pg|
            pg.should be @client
            @client.transaction_status.should be PG::PQTRANS_INTRANS
            @client.instance_variable_get(:@client_tran_count).should eq 3
            @client.query("INSERT INTO bar (key,value) VALUES(1000000,'million') returning value")
          end
        end
      end.tap do |result|
        result.should be_an_instance_of PG::Result
        result[0]['value'].should eq 'million'
      end
      @client.query('SELECT key,value FROM bar ORDER BY key') do |result|
        result.should be_an_instance_of PG::Result
        result.column_values(0).should eq ['42','100','1000','1000000']
        result.column_values(1).should eq ['xyz','hundred','thousand','million']
      end
      @client.transaction_status.should be PG::PQTRANS_IDLE
      @client.instance_variable_get(:@client_tran_count).should eq 0
    end

    it "should allow nesting transaction and rollback from the innermost block error" do
      @client.transaction_status.should be PG::PQTRANS_IDLE
      expect do
        @client.transaction do |pg|
          pg.should be @client
          @client.transaction_status.should be PG::PQTRANS_INTRANS
          @client.instance_variable_get(:@client_tran_count).should eq 1
          @client.query(
            "INSERT INTO bar (key,value) VALUES(200,'two hundred') returning value"
          ).should be_an_instance_of PG::Result
          @client.transaction do |pg|
            pg.should be @client
            @client.transaction_status.should be PG::PQTRANS_INTRANS
            @client.instance_variable_get(:@client_tran_count).should eq 2
            @client.query(
              "INSERT INTO bar (key,value) VALUES(2000,'two thousands') returning value"
            ).should be_an_instance_of PG::Result
            @client.transaction do |pg|
              pg.should be @client
              @client.transaction_status.should be PG::PQTRANS_INTRANS
              @client.instance_variable_get(:@client_tran_count).should eq 3
              @client.query(
                "INSERT INTO bar (key,value) VALUES(2000000,'two millions') returning value"
              ).should be_an_instance_of PG::Result
              raise "rollback from here"
            end
          end
        end
      end.to raise_error(RuntimeError, /rollback from here/)
      @client.query('SELECT key,value FROM bar ORDER BY key') do |result|
        result.should be_an_instance_of PG::Result
        result.column_values(0).should eq ['42','100','1000','1000000']
        result.column_values(1).should eq ['xyz','hundred','thousand','million']
      end
      @client.transaction_status.should be PG::PQTRANS_IDLE
      @client.instance_variable_get(:@client_tran_count).should eq 0
    end

    it "should allow rollback on rescued sql error from nested transaction" do
      flag = false
      @client.transaction_status.should be PG::PQTRANS_IDLE
      @client.transaction do |pg|
        pg.should be @client
        @client.transaction_status.should be PG::PQTRANS_INTRANS
        @client.instance_variable_get(:@client_tran_count).should eq 1
        @client.query(
          "INSERT INTO bar (key,value) VALUES(300,'three hundred') returning value"
        ).should be_an_instance_of PG::Result
        @client.transaction do |pg|
          pg.should be @client
          @client.transaction_status.should be PG::PQTRANS_INTRANS
          @client.instance_variable_get(:@client_tran_count).should eq 2
          @client.query(
            "INSERT INTO bar (key,value) VALUES(3000,'three thousands') returning value"
          ).should be_an_instance_of PG::Result
          @client.transaction do |pg|
            pg.should be @client
            @client.transaction_status.should be PG::PQTRANS_INTRANS
            @client.instance_variable_get(:@client_tran_count).should eq 3
            expect {
              @client.query('SRELECT CURRENT_TIMESTAMP')
            }.to raise_error(PG::EM::Errors::QueryError, /syntax error/)
            @client.transaction_status.should be PG::PQTRANS_INERROR
            @client.instance_variable_get(:@client_tran_count).should eq 3
            expect {
              @client.query('SELECT CURRENT_TIMESTAMP')
            }.to raise_error(PG::EM::Errors::QueryError, /transaction is aborted/)
            @client.transaction_status.should be PG::PQTRANS_INERROR
            @client.instance_variable_get(:@client_tran_count).should eq 3
            expect do
              @client.transaction { 'foo' }
            end.to raise_error(PG::EM::Errors::TransactionError, /error in transaction, need ROLLBACK/)
            @client.transaction_status.should be PG::PQTRANS_INERROR
            @client.instance_variable_get(:@client_tran_count).should eq 3
            flag = :was_here
          end
          @client.transaction_status.should be PG::PQTRANS_INERROR
          @client.instance_variable_get(:@client_tran_count).should eq 2
          expect {
            @client.query('SELECT CURRENT_TIMESTAMP')
          }.to raise_error(PG::EM::Errors::QueryError, /transaction is aborted/)
          expect do
            @client.transaction { 'foo' }
          end.to raise_error(PG::EM::Errors::TransactionError, /error in transaction, need ROLLBACK/)
          @client.transaction_status.should be PG::PQTRANS_INERROR
          @client.instance_variable_get(:@client_tran_count).should eq 2
        end
        @client.transaction_status.should be PG::PQTRANS_INERROR
        @client.instance_variable_get(:@client_tran_count).should eq 1
        expect {
          @client.query('SELECT CURRENT_TIMESTAMP')
        }.to raise_error(PG::EM::Errors::QueryError, /transaction is aborted/)
        expect do
          @client.transaction { 'foo' }
        end.to raise_error(PG::EM::Errors::TransactionError, /error in transaction, need ROLLBACK/)
        @client.transaction_status.should be PG::PQTRANS_INERROR
        @client.instance_variable_get(:@client_tran_count).should eq 1
      end
      @client.transaction_status.should be PG::PQTRANS_IDLE
      @client.instance_variable_get(:@client_tran_count).should eq 0
      @client.transaction { 'foo' }.should eq 'foo'
      @client.query('SELECT key,value FROM bar ORDER BY key') do |result|
        result.should be_an_instance_of PG::Result
        result.column_values(0).should eq ['42','100','1000','1000000']
        result.column_values(1).should eq ['xyz','hundred','thousand','million']
      end
      flag.should be :was_here
    end

    it "should detect premature transaction state change" do
      flag = false
      @client.transaction_status.should be PG::PQTRANS_IDLE
      expect do
        @client.transaction do |pg|
          pg.should be @client
          @client.transaction_status.should be PG::PQTRANS_INTRANS
          @client.instance_variable_get(:@client_tran_count).should eq 1
          @client.query('ROLLBACK')
          expect do
            @client.transaction { 'foo' }
          end.to raise_error(PG::EM::Errors::TransactionError, /^unable to begin nested transaction, current transaction was finished prematurely$/)
        end
      end.to raise_error(PG::EM::Errors::TransactionError, /^transaction was finished prematurely$/)
    end
  end

  around(:each) do |testcase|
    @after_em_stop = nil
    EM.synchrony do
      begin
        testcase.call
      ensure
        EM.stop
      end
    end
    @after_em_stop.call if @after_em_stop
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

end
