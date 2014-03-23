require 'coveralls/rake/task'
Coveralls::RakeTask.new
task :test_with_coveralls => ['test:all', 'coveralls:push']

$:.unshift "lib"

task :default => [:test]

$gem_name = "em-pg-client"

def windows_os?
  RbConfig::CONFIG['host_os'] =~ /cygwin|mswin|mingw|bccwin|wince|emx/
end

desc "Run tests"
task :test => :'test:safe'

namespace :test do
  env_common = {'PGDATABASE' => 'test'}
  env_unix = env_common.merge('PGHOST' => ENV['PGHOST_UNIX'] || '/tmp')
  env_inet = env_common.merge('PGHOST' => ENV['PGHOST_INET'] || 'localhost')

  task :warn do
    puts "WARNING: The tests needs to be run with an available local PostgreSQL server"
  end

  desc "Run specs only"
  task :spec => [:spec_defer, :spec_pool, :spec_client]

  task :spec_client do
    sh({'COVNAME'=>'spec:client'},     "rspec spec/pg_em_client_*.rb")
  end

  task :spec_pool do
    sh({'COVNAME'=>'spec:pool'},       "rspec spec/pg_em_connection_pool.rb")
  end

  task :spec_defer do
    sh({'COVNAME'=>'spec:deferrable'}, "rspec spec/pg_em_featured_deferrable.rb")
  end

  desc "Run safe tests only"
  task :safe  => [:warn, :spec, :async, :fiber, :on_connect, :pool]
  task :async => [:async_inet, :async_unix]
  task :fiber => [:fiber_inet, :fiber_unix]
  task :on_connect => [:on_connect_inet, :on_connect_unix]
  task :pool => [:pool_inet, :pool_unix]

  task :pool_inet do
    sh env_inet.merge('COVNAME'=>'pool:inet'), "rspec spec/em_connection_pool.rb"
  end

  task :pool_unix do
    sh env_unix.merge('COVNAME'=>'pool:unix'), "rspec spec/em_connection_pool.rb" unless windows_os?
  end

  task :on_connect_inet do
    sh env_inet.merge('COVNAME'=>'on_connect:inet'), "rspec spec/em_client_on_connect.rb"
  end

  task :on_connect_unix do
    sh env_unix.merge('COVNAME'=>'on_connect:unix'), "rspec spec/em_client_on_connect.rb" unless windows_os?
  end

  task :async_inet do
    sh env_inet.merge('COVNAME'=>'async:inet'), "rspec spec/em_client.rb"
  end

  task :async_unix do
    sh env_unix.merge('COVNAME'=>'async:unix'), "rspec spec/em_client.rb" unless windows_os?
  end

  task :fiber_inet do
    sh env_inet.merge('COVNAME'=>'fiber:inet'), "rspec spec/em_synchrony_client.rb"
  end

  task :fiber_unix do
    sh env_unix.merge('COVNAME'=>'fiber:unix'), "rspec spec/em_synchrony_client.rb" unless windows_os?
  end

  task :pgdata_check do
    unless ENV['PGDATA'] || (ENV['PG_CTL_STOP_CMD'] && ENV['PG_CTL_START_CMD'])
      raise "Set PGDATA environment variable before running the autoreconnect tests."
    end
  end

  desc "Run unsafe tests only"
  task :unsafe => [:warn, :pgdata_check,
    :async_autoreconnect_inet,
    :async_autoreconnect_unix,
    :fiber_autoreconnect_inet,
    :fiber_autoreconnect_unix]

  task :async_autoreconnect_inet do
    sh env_inet.merge('COVNAME'=>'async:autoreconnect:inet'), "rspec spec/em_client_autoreconnect.rb"
  end

  task :async_autoreconnect_unix do
    sh env_unix.merge('COVNAME'=>'async:autoreconnect:unix'), "rspec spec/em_client_autoreconnect.rb"
  end

  task :fiber_autoreconnect_inet do
    sh env_inet.merge('COVNAME'=>'fiber:autoreconnect:inet'), "rspec spec/em_synchrony_client_autoreconnect.rb"
  end

  task :fiber_autoreconnect_unix do
    sh env_unix.merge('COVNAME'=>'fiber:autoreconnect:unix'), "rspec spec/em_synchrony_client_autoreconnect.rb"
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

desc "Console"
task :console do
  require 'irb'
  require 'irb/completion'
  require 'em-synchrony'
  require 'em-pg-client'
  require 'pg/em/connection_pool'
  require 'pg/em/iterable'
  ARGV.clear
  IRB.start
end
