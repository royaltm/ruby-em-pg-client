if ENV["COVERAGE"]
  require 'simplecov'
  require 'coveralls'

  SimpleCov.start do
    add_filter "/spec/"
  end
  SimpleCov.command_name ENV['COVNAME'] || 'RSpec'
end
RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
  config.mock_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end
