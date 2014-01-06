# Demonstation of fully asynchronous query data streaming.
#
# This is a low-level method which has its:
#
# upside:   it's the same way you would work with PG::Connection
# downside: it's a little verbose and doesn't support automatic re-connects
gem 'em-pg-client', '>= 0.3.2'
require 'pg/em/connection_pool'
require 'em-synchrony'
require 'em-synchrony/fiber_iterator'

TABLE_NAME = 'resources'

unless PG::EM::Client.single_row_mode?
  raise 'compile pg against pqlib >= 9.2 to support single row mode'
end

EM.synchrony do
  EM.add_periodic_timer(0.01) { print ' ' }

  db = PG::EM::ConnectionPool.new size: 3

  10.times do

    EM::Synchrony::FiberIterator.new(%w[@ * #], 3).each do |mark|

      db.hold do |pg|
        pg.send_query("select * from #{TABLE_NAME}")
        pg.set_single_row_mode
        rows = 0
        while result = pg.get_result
          begin
            result.check
            result.each do |tuple|
              rows += 1
              # process tuple
              print mark
              # break stream cleanly
              pg.reset if rows > 1000
            end
          rescue PG::Error => e
            # cleanup connection
            pg.get_last_result
            raise e
          ensure
            result.clear
          end
        end
      end

    end

    puts
    puts '='*80
  end
  EM.stop
end
