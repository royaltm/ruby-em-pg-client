$:.unshift "lib"
gem 'eventmachine', '= 0.12.10'
require 'date'
require 'eventmachine'
require 'pg/em'

describe PG::EM::Client do

  it "should create simple table `foo`" do
    @client.query('DROP TABLE IF EXISTS foo') do |result|
      raise result if result.is_a? ::Exception
      @client.query('CREATE TABLE foo (id integer,cdate timestamp with time zone,data varchar)') do |result|
        raise result if result.is_a? ::Exception
        EM.stop
      end
    end
  end

  it "should populate foo with some data " do
    values = @values.dup
    results = []
    do_query = proc do
      data, id = values.shift
      @client.query('INSERT INTO foo (id,cdate,data) VALUES($1,$2,$3) returning cdate', [id, DateTime.now, data]) do |result|
        raise result if result.is_a? ::Exception
        results << DateTime.parse(result[0]['cdate'])
        if values.empty?
          @cdates.replace results
          results.length.should == @values.length
          results.each {|r| r.class.should == DateTime }
          EM.stop
        else
          do_query.call
        end
      end
    end
    do_query.call
  end

  it "should read foo table with prepared statement" do
    @client.prepare('get_foo', 'SELECT * FROM foo order by id') do
      @client.exec_prepared('get_foo') do |result|
        raise result if result.is_a? ::Exception
        result.each_with_index do |row, i|
          row['id'].to_i.should == i
          DateTime.parse(row['cdate']).should == @cdates[i]
          row['data'].should == @values[i][0]
        end
        EM.stop
      end
    end
  end
  
  around(:each) do |testcase|
    EM.run &testcase
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
