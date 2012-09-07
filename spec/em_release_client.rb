$:.unshift "lib"
gem 'eventmachine', '= 0.12.10'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'date'
require 'eventmachine'
require 'pg/em'
require 'em_client_common'
RSpec.configure do |config|
  config.include(PGSpecMacros)
end

describe PG::EM::Client do

  include_context 'em-pg common before'

  it "should populate foo with some data " do
    values = @values.dup
    results = []
    do_query = proc do
      data, id = values.shift
      @client.query('INSERT INTO foo (id,cdate,data) VALUES($1,$2,$3) returning cdate', [id, DateTime.now, data]) do |result|
        result.should be_an_instance_of PG::Result
        results << DateTime.parse(result[0]['cdate'])
        if values.empty?
          @cdates.replace results
          results.length.should == @values.length
          results.each {|r| r.should be_an_instance_of DateTime }
          EM.stop
        else
          do_query.call
        end
      end.should be_a_kind_of ::EM::DefaultDeferrable
    end
    do_query.call
  end

  include_context 'em-pg common after'

end
