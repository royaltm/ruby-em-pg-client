$:.unshift "lib"
gem 'eventmachine', '>= 1.0.0.beta.1'
require 'date'
require 'eventmachine'
require 'pg/em'
require 'em_client_common'

describe PG::EM::Client do

  include_context 'em-pg common before'

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

  include_context 'em-pg common after'

end
