$:.unshift "lib"
require 'date'
require 'em-synchrony'
require 'em-synchrony/pg'

describe PG::EM::Client do

  it "should create simple table `foo`" do
    @client.query('DROP TABLE IF EXISTS foo')
    @client.query('CREATE TABLE foo (id integer,cdate timestamp with time zone,data varchar)')
    EM.stop
  end

  it "should populate foo with some data " do
    results = @values.map do |(data, id)|
      @client.query('INSERT INTO foo (id,cdate,data) VALUES($1,$2,$3) returning cdate', [id, DateTime.now, data]) do |result|
        DateTime.parse(result[0]['cdate'])
      end
    end
    @cdates.replace results
    results.length.should == @values.length
    results.each {|r| r.class.should == DateTime }
    EM.stop
  end

  it "should read foo table with prepared statement" do
    @client.prepare('get_foo', 'SELECT * FROM foo order by id')
    @client.exec_prepared('get_foo') do |result|
      result.each_with_index do |row, i|
        row['id'].to_i.should == i
        DateTime.parse(row['cdate']).should == @cdates[i]
        row['data'].should == @values[i][0]
      end
    end
    EM.stop
  end

  around(:each) do |testcase|
    EM.synchrony &testcase
  end

  before(:all) do
    @cdates = []
    @values = Array(('AA'..'ZZ').each_with_index)
    @client = PG::EM::Client.new(dbname: 'test')
    @client.query 'BEGIN TRANSACTION'
  end

  after(:all) do
    @client.query 'ROLLBACK TRANSACTION'
    @client.close
  end

end
