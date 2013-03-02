$:.unshift "lib"
gem 'eventmachine', '>= 1.0.0.beta.1'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'date'
require 'eventmachine'
require 'pg/em'

describe PG::EM::Errors do
  it "should initialize PGError with connection and result" do
    %w[PGError QueryError ConnectError QueryTimeoutError ConnectTimeoutError].each do |err_name|
      err_class = described_class.const_get(err_name)
      err = err_class.new('error')
      err.should be_a_kind_of PG::Error
      err.connection.should be_nil
      err.result.should be_nil
      err = err_class.new('error', :connection)
      err.should be_a_kind_of PG::Error
      err.connection.should eq :connection
      err.result.should be_nil
      err = err_class.new('error', :connection, :result)
      err.should be_a_kind_of PG::Error
      err.connection.should eq :connection
      err.result.should eq :result
    end
  end

  it "should allow rescue of TimeoutError" do
    %w[QueryTimeoutError ConnectTimeoutError].each do |err_name|
      err_class = described_class.const_get(err_name)
      begin
        raise err_class
      rescue described_class::TimeoutError => e
        e.should be_an_instance_of err_class
      end
    end
  end

  it "should wrap PG::Error" do
    %w[PGError QueryError ConnectError QueryTimeoutError ConnectTimeoutError].each do |err_name|
      err_class = described_class.const_get(err_name)
      begin
        error = PG::Error.new
        error.instance_variable_set(:@connection, :connection)
        error.instance_variable_set(:@result, :result)
        raise error
      rescue PG::Error => e
        e.connection.should eq :connection
        e.result.should eq :result
        begin
          wrapped = err_class.wrap(e)
          wrapped.should_not be e
          raise wrapped
        rescue err_class => ee
          ee.should_not be e
          ee.message.should eq e.message
          ee.backtrace.should eq e.backtrace
          ee.connection.should eq :connection
          ee.result.should eq :result
        end
      end
    end
  end

  it "should not wrap any other error" do
    %w[PGError QueryError ConnectError QueryTimeoutError ConnectTimeoutError].each do |err_name|
      err_class = described_class.const_get(err_name)
      [described_class::PGError, ArgumentError, RuntimeError, StandardError, Exception].each do |err_other_class|
        begin
          raise err_other_class
        rescue Exception => e
          begin
            wrapped = err_class.wrap(e)
            wrapped.should be e
            raise wrapped
          rescue err_other_class => ee
            ee.should be e
            ee.message.should eq e.message
            ee.backtrace.should eq e.backtrace
          end
        end
      end
    end
  end

end
