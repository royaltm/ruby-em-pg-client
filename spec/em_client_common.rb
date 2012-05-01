shared_context 'em-pg common before' do

  around(:each) do |testcase|
    EM.run &testcase
  end

  before(:all) do
    @cdates = []
    @values = Array(('AA'..'ZZ').each_with_index)
    @client = described_class.new
    @client.query 'BEGIN TRANSACTION'
  end

  after(:all) do
    @client.query 'ROLLBACK TRANSACTION'
    @client.close
  end

  it "should be a client" do
    @client.should be_an_instance_of described_class
    EM.stop
  end

  it "should create simple table `foo`" do
    @client.query('DROP TABLE IF EXISTS foo') do |result|
      result.should be_an_instance_of PG::Result
      @client.query('CREATE TABLE foo (id integer,cdate timestamp with time zone,data varchar)') do |result|
        result.should be_an_instance_of PG::Result
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

end

shared_context 'em-pg common after' do

  it "should read foo table with prepared statement" do
    @client.prepare('get_foo', 'SELECT * FROM foo order by id') do |result|
      result.should be_an_instance_of PG::Result
      @client.exec_prepared('get_foo') do |result|
        result.should be_an_instance_of PG::Result
        result.each_with_index do |row, i|
          row['id'].to_i.should == i
          DateTime.parse(row['cdate']).should == @cdates[i]
          row['data'].should == @values[i][0]
        end
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should connect to database asynchronously" do
    this = :first
    described_class.async_connect do |conn|
      this = :second
      conn.should be_an_instance_of described_class
      conn.query('SELECT pg_database_size(current_database());') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end.should be_a_kind_of ::EM::DefaultDeferrable
    this.should be :first
  end

  it "should raise syntax error in misspelled multiple statement" do
    @client.query('SELECT * from pg_class; SRELECT CURRENT_TIMESTAMP; SELECT 42 number') do |result|
      result.should be_an_instance_of PG::Error
      result.to_s.should include "syntax error"
      @client.query('ROLLBACK') do |result|
        result.should be_an_instance_of PG::Result
        @client.query('BEGIN TRANSACTION') do |result|
          result.should be_an_instance_of PG::Result
          EM.stop
        end.should be_a_kind_of ::EM::DefaultDeferrable
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should return only last statement" do
    @client.query('SELECT * from pg_class; SELECT CURRENT_TIMESTAMP; SELECT 42 number') do |result|
      result.should be_an_instance_of PG::Result
      result[0]['number'].should eq "42"
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should timeout expire while executing query" do
    @client.query_timeout.should eq 0
    @client.query_timeout = 1.5
    @client.query_timeout.should eq 1.5
    start_time = Time.now
    @client.query('SELECT pg_sleep(2)') do |result|
      (Time.now - start_time).should be < 2
      result.should be_an_instance_of PG::Error
      result.to_s.should include "query timeout expired"
      @client.async_command_aborted.should be_true
      @client.query_timeout = 0
      @client.query_timeout.should eq 0
      @client.query('BEGIN TRANSACTION') do |result|
        result.should be_an_instance_of PG::Result
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end.should be_a_kind_of ::EM::DefaultDeferrable
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
      EM.stop
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should timeout expire while executing query with partial results" do
    @client.query_timeout.should eq 0
    @client.query_timeout = 1.1
    @client.query_timeout.should eq 1.1
    start_time = Time.now
    @client.query(
        'SELECT * from pg_class;' +
        'SELECT pg_sleep(1);' +
        'SELECT * from pg_class;' + 
        'SELECT pg_sleep(2);' +
        'SELECT 42 number') do |result|
      (Time.now - start_time).should be > 2
      result.should be_an_instance_of PG::Error
      result.to_s.should include "query timeout expired"
      @client.query_timeout = 0
      @client.query_timeout.should eq 0
      @client.query('BEGIN TRANSACTION') do |result|
        result.should be_an_instance_of PG::Result
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

end
