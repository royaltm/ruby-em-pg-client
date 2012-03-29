$:.unshift "lib"

Gem::Specification.new do |s|
  s.name = "em-pg-client"
  s.version = "0.1.0"
  s.required_ruby_version = ">= 1.9.1"
  s.date = "#{Time.now.strftime("%Y-%m-%d")}"
  s.summary = "EventMachine PostgreSQL driver"
  s.email = "rafal@yeondir.com"
  s.homepage = "http://github.com/royaltm/ruby-em-pg-client"
  s.require_path = "lib"
  s.description = "PostgreSQL asynchronous EventMachine client wrapper"
  s.authors = ["Rafal Michalski"]
  s.files = `git ls-files`.split("\n")
  s.test_files = Dir.glob("spec/**/*")
  s.rdoc_options = << "--title" << "EventMachine PostgreSQL client" <<
    "--main" << "README.rdoc"
  s.has_rdoc = true
  s.extra_rdoc_files = ["README.rdoc", "BENCHMARKS.rdoc"]
  s.requirements << "PostgreSQL server"
  s.add_runtime_dependency "pg", ">= 0.13.2"
  s.add_runtime_dependency "eventmachine", ">= 0.12.10"
  s.add_development_dependency "rspec", "~> 2.8.0"
  s.add_development_dependency "em-synchrony", "~> 1.0.0"
end
