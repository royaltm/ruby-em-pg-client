$:.unshift "lib"
require 'pg/em-version'

files = `git ls-files`.split("\n")

Gem::Specification.new do |s|
  s.name = "em-pg-client"
  s.version = PG::EM::VERSION
  s.required_ruby_version = ">= 1.9.3"
  s.date = "#{Time.now.strftime("%Y-%m-%d")}"
  s.summary = "EventMachine PostgreSQL client"
  s.email = "rafal@yeondir.com"
  s.homepage = "http://github.com/royaltm/ruby-em-pg-client"
  s.license = "MIT"
  s.require_path = "lib"
  s.description = "PostgreSQL asynchronous EventMachine client, based on pg interface (PG::Connection)"
  s.authors = ["Rafal Michalski"]
  s.files = files - ['.gitignore']
  s.test_files = Dir.glob("spec/**/*")
  s.rdoc_options << "--title" << "em-pg-client" <<
    "--main" << "README.md"
  s.has_rdoc = true
  s.extra_rdoc_files = [
      files.grep(/^benchmarks\/.*\.rb$/),
      "README.md", "BENCHMARKS.md", "LICENSE", "HISTORY.md"
    ].flatten
  s.requirements << "PostgreSQL server"
  s.add_runtime_dependency "pg", ">= 0.17.0"
  s.add_runtime_dependency "eventmachine", "~> 1.2.0"
  s.add_development_dependency "rspec", "~> 3.4.0"
  s.add_development_dependency "em-synchrony", "~> 1.0.5"
  s.add_development_dependency "coveralls", ">= 0.8.13"
  s.add_development_dependency "simplecov", ">= 0.11.2"
end
