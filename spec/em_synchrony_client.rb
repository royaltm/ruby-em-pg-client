$:.unshift "lib"
require 'date'
require 'em-synchrony'
require 'em-synchrony/pg'

describe PG::EM::Client do

  it "should be client" do
    @client.should be_an_instance_of described_class
  end

  it "should create simple table `foo`" do
    @client.query(
      'DROP TABLE IF EXISTS foo'
    ).should be_an_instance_of PG::Result
    @client.query(
      'CREATE TABLE foo (id integer,cdate timestamp with time zone,data varchar)'
    ).should be_an_instance_of PG::Result
  end

  it "should populate foo with some data " do
    results = @values.map do |(data, id)|
      @client.query('INSERT INTO foo (id,cdate,data) VALUES($1,$2,$3) returning cdate', [id, DateTime.now, data]) do |result|
        result.should be_an_instance_of PG::Result
        DateTime.parse(result[0]['cdate'])
      end
    end
    @cdates.replace results
    results.length.should == @values.length
    results.each {|r| r.should be_an_instance_of DateTime }
  end

  it "should read foo table with prepared statement" do
    @client.prepare('get_foo', 
      'SELECT * FROM foo order by id'
    ).should be_an_instance_of PG::Result
    @client.exec_prepared('get_foo') do |result|
      result.should be_an_instance_of PG::Result
      result.each_with_index do |row, i|
        row['id'].to_i.should == i
        DateTime.parse(row['cdate']).should == @cdates[i]
        row['data'].should == @values[i][0]
      end
    end
  end

  it "should connect to database asynchronously" do
    this = :first
    f = Fiber.current
    Fiber.new do
      conn = described_class.new
      this = :second
      conn.should be_an_instance_of described_class
      conn.query('SELECT pg_database_size(current_database());') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
      end
      conn.close
      f.resume
    end.resume
    this.should be :first
    Fiber.yield
    this.should be :second
  end

  it "should raise syntax error in misspelled multiple statement" do
    expect {
      @client.query('SELECT * from pg_class; SRELECT CURRENT_TIMESTAMP; SELECT 42 number')
    }.to raise_error(PG::Error, /syntax error/)
    @client.query('ROLLBACK') do |result|
      result.should be_an_instance_of PG::Result
    end
    @client.query('BEGIN TRANSACTION') do |result|
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
    }.to raise_error(PG::Error, /query timeout expired/)
    (Time.now - start_time).should be < 2
    @client.query_timeout = 0
    @client.query_timeout.should eq 0
    @client.async_command_aborted.should be_true
    @client.query('BEGIN TRANSACTION') do |result|
      result.should be_an_instance_of PG::Result
    end
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
    }.to raise_error(PG::Error, /query timeout expired/)
    (Time.now - start_time).should be > 2
    @client.query_timeout = 0
    @client.query_timeout.should eq 0
    @client.query('BEGIN TRANSACTION') do |result|
      result.should be_an_instance_of PG::Result
    end
  end

  around(:each) do |testcase|
    EM.synchrony do
      begin
        testcase.call
      ensure
        EM.stop
      end
    end
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

end
