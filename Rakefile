$:.unshift "lib"

task :default => [:test]

$gem_name = "em-pg-client"

desc "Run tests"
task :test => :'test:safe'

namespace :test do
  env_common = {'PGDATABASE' => 'test'}
  env_pg_013 = {'EM_PG_CLIENT_TEST_PG_VERSION' => '= 0.13.2'}
  env_unix_socket = env_common.merge('PGHOST' => '/tmp')
  env_tcpip = env_common.merge('PGHOST' => 'localhost')

  task :warn do
    puts "WARNING: The test needs to be run with an available local PostgreSQL server"
  end

  desc "Run specs only"
  task :spec do
    sh "rspec spec/pg_em_featured_deferrable.rb"
  end

  desc "Run safe tests only"
  task :safe => [:warn, :spec] do
    %w[
      spec/em_release_client.rb
      spec/em_devel_client.rb
      spec/em_synchrony_client.rb
    ].each do |spec|
      sh env_unix_socket, "rspec #{spec}"
      sh env_tcpip, "rspec #{spec}"
      sh env_pg_013.merge(env_unix_socket), "rspec #{spec}"
      sh env_pg_013.merge(env_tcpip), "rspec #{spec}"
    end
  end

  desc "Run unsafe tests only"
  task :unsafe => :warn do
    raise "Set PGDATA environment variable before running the autoreconnect tests." unless ENV['PGDATA']
    %w[
      spec/em_client_autoreconnect.rb
      spec/em_synchrony_client_autoreconnect.rb
    ].each do |spec|
      sh env_unix_socket, "rspec #{spec}"
      sh env_tcpip, "rspec #{spec}"
    end
  end

  desc "Run safe and unsafe tests"
  task :all => [:spec, :safe, :unsafe]
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
  sh "yardoc"
end

desc "Benchmark"
task :benchmark do
  require "./benchmarks/em_pg.rb"
  [10, 100, 1000].each do |i|
    puts "Repeat: #{i}"
    benchmark(i)
  end
end
