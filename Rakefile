$:.unshift "lib"

task :default => [:test]

$gem_name = "em-pg-client"

desc "Run spec tests"
task :test, [:which] do |t, args|
  args.with_defaults(:which => 'safe')

  env_unix_socket = {'PGDATABASE' => 'test', 'PGHOST' => '/tmp'}
  env_tcpip = {'PGDATABASE' => 'test', 'PGHOST' => 'localhost'}

  puts "WARNING: The test needs to be run with an available local PostgreSQL server"

  if %w[all safe].include? args[:which]
    %w[
      spec/em_release_client.rb
      spec/em_devel_client.rb
      spec/em_synchrony_client.rb
    ].each do |spec|
      sh env_unix_socket, "rspec #{spec}"
      sh env_tcpip, "rspec #{spec}"
    end
  end

  if %w[all unsafe dangerous autoreconnect].include? args[:which]
    raise "Set PGDATA environment variable before running the autoreconnect tests." unless ENV['PGDATA']
    %w[
      spec/em_client_autoreconnect.rb
      spec/em_synchrony_client_autoreconnect.rb
    ].each do |spec|
      sh env_unix_socket, "rspec #{spec}"
      sh env_tcpip, "rspec #{spec}"
    end
  end
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
