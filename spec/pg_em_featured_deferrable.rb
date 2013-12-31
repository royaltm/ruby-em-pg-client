$:.unshift "lib"
gem 'eventmachine', '~> 1.0.0'
gem 'pg', ENV['EM_PG_CLIENT_TEST_PG_VERSION']
require 'date'
require 'eventmachine'
require 'pg/em'

describe PG::EM::FeaturedDeferrable do
  subject           { PG::EM::FeaturedDeferrable }
  let(:df)          { subject.new }
  let(:cb)          { Proc.new {} }
  let(:error)       { RuntimeError.new }
  let(:pg_error)    {
    PG::Error.new.tap do |err|
      err.instance_variable_set :@connection, :connection
      err.instance_variable_set :@result, :result
    end
  }

  it "should set callback with a block " do
    cb.should_receive(:call).with(:result)
    df = subject.new(&cb)
    df.succeed(:result)
  end

  it "should set errback with a block" do
    cb.should_receive(:call).with(:err)
    df = subject.new(&cb)
    df.fail(:err)
  end

  it "should execute callbacks and errbacks in setup order" do
    results = []
    setup_callbacks = proc do |df|
      df.callback { results << 1 }
      df.callback { results << 2 }
      df.callback { results << 3 }
      df.errback  { results << 4 }
      df.errback  { results << 5 }
      df.errback  { results << 6 }
    end
    df = subject.new
    setup_callbacks.call df
    df.succeed
    df.fail
    df = subject.new
    setup_callbacks.call df
    df.fail
    df.succeed
    results.should eq [1, 2, 3, 4, 5, 6]
  end

  it "should set completion with block" do
    cb.should_receive(:call).with(:err)
    df = subject.new
    df.completion(&cb)
    df.fail(:err)
    df.succeed(:result)

    cb.should_receive(:call).with(:result)
    df = subject.new
    df.completion(&cb)
    df.succeed(:result)
    df.fail(:err)
  end

  it "should bind status to another deferrable" do
    cb.should_receive(:call).with(:result)
    df = subject.new(&cb)
    other_df = subject.new
    df.bind_status(other_df)
    other_df.succeed(:result)

    cb.should_receive(:call).with(:err)
    df = subject.new(&cb)
    other_df = subject.new
    df.bind_status(other_df)
    other_df.fail(:err)
  end

  shared_context 'shared protect' do
    it "should call df.fail and return nil" do
      ::EM.stub(:next_tick) {|&cb| cb.call }
      df.errback(&cb)
      cb.should_receive(:call).with(error)
      df.send(protect_method) do
        raise error
      end.should be_nil
    end

    it "should call df.fail and return custom fail value" do
      ::EM.stub(:next_tick) {|&cb| cb.call }
      df.errback(&cb)
      cb.should_receive(:call).with(error)
      df.send(protect_method, :fail) do
        raise error
      end.should eq :fail
    end
  end

  context "#protect" do
    let(:protect_method) { :protect }
    it "should return value" do
      df.protect do
        :result
      end.should eq :result
    end

    include_context 'shared protect'
  end

  context "#protect_and_succeed" do
    let(:protect_method) { :protect_and_succeed }
    it "should call deferrable.succeed and return value" do
      ::EM.stub(:next_tick) {|&cb| cb.call }
      df.callback(&cb)
      cb.should_receive(:call).with(:result)
      df.protect_and_succeed do
        :result
      end.should eq :result
    end

    include_context 'shared protect'
  end
end
