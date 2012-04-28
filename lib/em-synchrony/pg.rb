require 'pg/em'
module PG
  module EM
    class Client
      # Author:: Rafal Michalski (mailto:royaltm75@gmail.com)
      # Licence:: MIT License
      #
      # =PostgreSQL Client for EM-Synchrony
      #

      # conform to *standard*
      alias_method :aquery, :async_query

      # fiber untangled version of theese methods:
      # - exec (aliased as query)
      # - exec_prepared
      # - prepare
      %w(exec exec_prepared prepare reset self.connect).each do |name|
        async_name = "async_#{name.split('.').last}"
        class_eval <<-EOD
          def #{name}(*args, &blk)
            if ::EM.reactor_running?
              df = #{async_name}(*args)
              f = Fiber.current
              df.callback { |res| f.resume(res) }
              df.errback  { |err| f.resume(err) }

              result = Fiber.yield
              raise result if result.is_a?(::Exception)
              if block_given?
                yield result 
              else
                result
              end
            else
              super(*args, &blk)
            end
          end
        EOD
      end

      class << self
        alias_method :new, :connect
        alias_method :open, :connect
        alias_method :setdb, :connect
        alias_method :setdblogin, :connect
      end

      alias_method :query, :exec

    end
  end
end
