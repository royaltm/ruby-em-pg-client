$:.unshift "lib"
gem 'eventmachine', '~> 1.2.0'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'date'
require 'eventmachine'
require 'pg/em'

describe 'em-pg-client options' do
  subject                   { PG::EM::Client }

  let(:callback)            { proc {|c, e| false } }
  let(:args)                { [{
     query_timeout: 1,   async_autoreconnect: true,   connect_timeout: 10,   host: 'foo'
  }]}
  let(:str_key_args)        { [{
    'query_timeout'=>1, 'async_autoreconnect'=>true, 'connect_timeout'=>10, 'host'=>'foo'
  }]}
  let(:pgconn_args)         { [{connect_timeout: 10, host: 'foo'}] }
  let(:str_key_pgconn_args) { [{'connect_timeout'=>10, 'host'=>'foo'}] }
  let(:async_options)       { {
                                :@async_autoreconnect => true,
                                :@connect_timeout => 10,
                                :@query_timeout=>1,
                                :@on_connect=>nil,
                                :@on_autoreconnect=>nil,
                                :@async_command_aborted=>false} }

  it "should parse options and not modify original hash" do
    orig_args = args.dup
    orig_options = orig_args.first.dup
    options = subject.parse_async_options orig_args
    options.should eq async_options
    orig_args.should eq pgconn_args
    args.first.should eq orig_options
  end

  it "should parse options with keys as strings" do
    orig_args = str_key_args.dup
    orig_options = orig_args.first.dup
    options = subject.parse_async_options orig_args
    options.should eq async_options
    orig_args.should eq str_key_pgconn_args
    str_key_args.first.should eq orig_options
  end

  it "should set async_autoreconnect according to on_autoreconnect" do
    options = subject.parse_async_options []
    options.should be_an_instance_of Hash
    options[:@on_autoreconnect].should be_nil
    options[:@async_autoreconnect].should be_false

    args = [on_autoreconnect: callback]
    options = subject.parse_async_options args
    args.should eq [{}]
    options.should be_an_instance_of Hash
    options[:@on_autoreconnect].should be callback
    options[:@async_autoreconnect].should be_true

    args = [async_autoreconnect: false,
      on_autoreconnect: callback]
    options = subject.parse_async_options args
    args.should eq [{}]
    options.should be_an_instance_of Hash
    options[:@on_autoreconnect].should be callback
    options[:@async_autoreconnect].should be_false

    args = [on_autoreconnect: callback,
      async_autoreconnect: false]
    options = subject.parse_async_options args
    args.should eq [{}]
    options.should be_an_instance_of Hash
    options[:@on_autoreconnect].should be callback
    options[:@async_autoreconnect].should be_false
  end

  it "should set only callable on_autoreconnect" do
    expect do
      subject.parse_async_options [on_autoreconnect: true]
    end.to raise_error(ArgumentError, /must respond to/)

    expect do
      subject.parse_async_options ['on_autoreconnect' => Object.new]
    end.to raise_error(ArgumentError, /must respond to/)

    options = subject.parse_async_options [on_autoreconnect: callback]
    options.should be_an_instance_of Hash
    options[:@on_autoreconnect].should be callback
  end

  it "should set only callable on_connect" do
    expect do
      subject.parse_async_options [on_connect: true]
    end.to raise_error(ArgumentError, /must respond to/)

    expect do
      subject.parse_async_options ['on_connect' => Object.new]
    end.to raise_error(ArgumentError, /must respond to/)

    options = subject.parse_async_options [on_connect: callback]
    options.should be_an_instance_of Hash
    options[:@on_connect].should be callback
  end

  it "should raise error with obsolete argument" do
    expect do
      subject.parse_async_options [on_reconnect: true]
    end.to raise_error ArgumentError
  end

  it "should set on_* options with a writer or a block" do
    async_args = subject.parse_async_options([])
    client = subject.allocate
    client.instance_eval {
      async_args.each {|k, v| instance_variable_set(k, v) }
    }
    client.should be_an_instance_of subject
    client.on_connect.should be_nil
    client.on_autoreconnect.should be_nil
    client.on_connect = callback
    client.on_connect.should be callback
    client.on_autoreconnect = callback
    client.on_autoreconnect.should be callback
    client.on_connect = nil
    client.on_connect.should be_nil
    client.on_autoreconnect = nil
    client.on_autoreconnect.should be_nil
    client.on_connect(&callback).should be callback
    client.on_connect.should be callback
    client.on_autoreconnect(&callback).should be callback
    client.on_autoreconnect.should be callback
  end

end