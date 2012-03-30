$:.unshift "lib"

task :default => [:test]

$gem_name = "em-pg-client"

desc "Run tests"
task :test do
  puts "WARNING: The test needs to be run with an available local PostgreSQL server"
  sh "rspec spec/em_release_client.rb"
  sh "rspec spec/em_devel_client.rb"
  sh "rspec spec/em_synchrony_client.rb"
end

desc "Build the gem"
task :gem do
  sh "gem build #$gem_name.gemspec"
end

desc "Install the library at local machnie"
task :install => :gem do 
  sh "gem install #$gem_name -l"
end

desc "Uninstall the library from local machnie"
task :uninstall do
  sh "gem uninstall #$gem_name"
end

desc "Clean"
task :clean do
  sh "rm #$gem_name*.gem" 
end

desc "Documentation"
task :doc do
  sh "rdoc --encoding=UTF-8 --title=em-pg-client --main=README.rdoc README.rdoc BENCHMARKS.rdoc lib/*/*.rb"
end

desc "Benchmark"
task :benchmark do
  require "./benchmarks/em_pg.rb"
  [10, 100, 1000].each do |i|
    puts "Repeat: #{i}"
    benchmark(i)
  end
end
