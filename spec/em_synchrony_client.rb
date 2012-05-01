$:.unshift "lib"
require 'date'
require 'em-synchrony'
require 'em-synchrony/pg'

describe PG::EM::Client do

  it "should create simple table `foo`" do
    @client.query(
      'DROP TABLE IF EXISTS foo'
    ).should be_an_instance_of PG::Result
    @client.query(
      'CREATE TABLE foo (id integer,cdate timestamp with time zone,data varchar)'
    ).should be_an_instance_of PG::Result
    EM.stop
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
    EM.stop
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
    EM.stop
  end

  it "should connect to database asynchronously" do
    this = :first
    Fiber.new do
      conn = described_class.new
      this = :second
      conn.should be_an_instance_of described_class
      conn.query('SELECT pg_database_size(current_database());') do |result|
        result.should be_an_instance_of PG::Result
        result[0]['pg_database_size'].to_i.should be > 0
      end
      conn.close
      EM.stop
    end.resume
    this.should be :first
  end

  around(:each) do |testcase|
    EM.synchrony &testcase
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
