$:.unshift "lib"
require 'pg/em-version'

Gem::Specification.new do |s|
  s.name = "em-pg-client"
  s.version = PG::EM::VERSION
  s.required_ruby_version = ">= 1.9.1"
  s.date = "#{Time.now.strftime("%Y-%m-%d")}"
  s.summary = "EventMachine PostgreSQL client"
  s.email = "rafal@yeondir.com"
  s.homepage = "http://github.com/royaltm/ruby-em-pg-client"
  s.require_path = "lib"
  s.description = "PostgreSQL asynchronous EventMachine client, based on pg interface (PG::Connection)"
  s.authors = ["Rafal Michalski"]
  s.files = `git ls-files`.split("\n") - ['.gitignore']
  s.test_files = Dir.glob("spec/**/*")
  s.rdoc_options << "--title" << "em-pg-client" <<
    "--main" << "README.md"
  s.has_rdoc = true
  s.extra_rdoc_files = ["README.md", "BENCHMARKS.md", "LICENCE"]
  s.requirements << "PostgreSQL server"
  s.add_runtime_dependency "pg", ">= 0.17.0"
  s.add_runtime_dependency "eventmachine", ">= 0.12.10"
  s.add_development_dependency "rspec", "~> 2.8.0"
  s.add_development_dependency "eventmachine", ">= 1.0.0.beta.1"
  s.add_development_dependency "em-synchrony", "~> 1.0.0"
end
