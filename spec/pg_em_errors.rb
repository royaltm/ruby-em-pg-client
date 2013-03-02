$:.unshift "lib"
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'eventmachine'
require 'pg/em'

describe PG::EM::Errors do
  let(:pg_error) {
    described_class::PGError
  }

  let(:pg_em_errors) {
    %w[Error QueryError ConnectionError QueryTimeoutError ConnectionTimeoutError].map do |err_name|
      described_class.const_get err_name
    end
  }

  let(:other_errors) {
    [described_class::Error, ArgumentError, RuntimeError, StandardError, Exception]
  }

  let(:timeout_errors) {
    %w[QueryTimeoutError ConnectionTimeoutError].map do |err_name|
      described_class.const_get err_name
    end
  }

  it "should initialize PGError with connection and result" do
    pg_em_errors.each do |err_class|
      err = err_class.new('error')
      err.should be_a_kind_of pg_error
      err.connection.should be_nil
      err.result.should be_nil
      err = err_class.new('error', :connection)
      err.should be_a_kind_of pg_error
      err.connection.should eq :connection
      err.result.should be_nil
      err = err_class.new('error', :connection, :result)
      err.should be_a_kind_of pg_error
      err.connection.should eq :connection
      err.result.should eq :result
    end
  end

  it "should allow rescue of TimeoutError" do
    timeout_errors.each do |err_class|
      begin
        raise err_class
      rescue described_class::TimeoutError => e
        e.should be_an_instance_of err_class
      end
    end
  end

  it "should wrap PG::Error" do
    pg_em_errors.each do |err_class|
      begin
        error = pg_error.new
        error.instance_variable_set(:@connection, :connection)
        error.instance_variable_set(:@result, :result)
        raise error
      rescue pg_error => e
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
    pg_em_errors.each do |err_class|
      other_errors.each do |err_other_class|
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
