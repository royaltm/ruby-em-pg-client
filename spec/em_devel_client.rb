$:.unshift "lib"
gem 'eventmachine', '>= 1.0.0.beta.1'
require 'date'
require 'eventmachine'
require 'pg/em'

describe PG::EM::Client do

  it "should create simple table `foo`" do
    @client.query('DROP TABLE IF EXISTS foo') do |result|
      result.should be_an_instance_of PG::Result
      @client.query('CREATE TABLE foo (id integer,cdate timestamp with time zone,data varchar)') do |result|
        result.should be_an_instance_of PG::Result
        EM.stop
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end.should be_a_kind_of ::EM::DefaultDeferrable
  end

  it "should populate foo with some data " do
    EM::Iterator.new(@values).map(proc{ |(data, id), iter|
      @client.query('INSERT INTO foo (id,cdate,data) VALUES($1,$2,$3) returning cdate', [id, DateTime.now, data]) do |result|
      result.should be_an_instance_of PG::Result
        iter.return(DateTime.parse(result[0]['cdate']))
      end.should be_a_kind_of ::EM::DefaultDeferrable
    }, proc{ |results|
      @cdates.replace results
      results.length.should == @values.length
      results.each {|r| r.should be_an_instance_of DateTime }
      EM.stop
    })
  end

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
end
