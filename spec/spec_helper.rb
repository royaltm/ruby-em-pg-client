if ENV["COVERAGE"]
  require 'simplecov'
  require 'coveralls'

  SimpleCov.start do
    add_filter "/spec/"
  end
  SimpleCov.command_name ENV['COVNAME'] || 'RSpec'
end
