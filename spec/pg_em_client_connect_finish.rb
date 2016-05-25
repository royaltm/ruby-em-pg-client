$:.unshift "lib"
gem 'eventmachine', '~> 1.2.0'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'eventmachine'
require 'em-synchrony'
require 'pg/em'

shared_context 'test deferred' do
  it "should not finish connection on deferred connection failure" do
    EM.run do
      subject.connect_defer(options) do |ex|
        ex.should be_an_instance_of PG::ConnectionBad
        ex.connection.should be_an_instance_of subject
        ex.connection.finished?.should be false
        EM.stop
      end.should be_a_kind_of ::EM::Deferrable
    end
  end
end

shared_context 'test blocking' do
  it "should not finish connection on blocking connection failure" do
    EM.synchrony do
      expect do
        subject.new(options)
      end.to raise_error(PG::ConnectionBad)
      begin
        subject.new(options)
      rescue => ex
        ex.should be_an_instance_of PG::ConnectionBad
        ex.connection.should be_an_instance_of subject
        ex.connection.finished?.should be false
      end
      EM.stop
    end
  end
end

describe 'connect failure and finished? status' do
  subject          { PG::EM::Client }
  let(:bogus_port) { 1 }

  describe 'with localhost'  do
    let(:options)  { {host: 'localhost', port: bogus_port} }
    include_context 'test deferred'
    include_context 'test blocking'
  end

  describe 'with unix socket' do
    let(:options)  { {host: ENV['PGHOST_UNIX'] || '/tmp', port: bogus_port} }
    include_context 'test deferred'
    include_context 'test blocking'
  end unless RSpec::Support::OS.windows?
end
