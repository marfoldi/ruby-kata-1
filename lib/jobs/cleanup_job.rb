require "csv"
require "sqlite3"

module Echocat
  class CleanupJob
    DB = SQLite3::Database.new "kata.db"

    def call
      DB.execute <<-SQL
      DROP TABLE IF EXISTS authors
      SQL
      DB.execute <<-SQL
        DROP TABLE IF EXISTS publications
      SQL
      DB.execute <<-SQL
        DROP TABLE IF EXISTS author_publication
      SQL
    end
  end
end
