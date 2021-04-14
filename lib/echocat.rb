# frozen_string_literal: true

require_relative "jobs/import_job"
require_relative "jobs/cleanup_job"

module Echocat
  DB = SQLite3::Database.new "kata.db"

  def self.run
    Echocat::ImportJob.new.()
    header = (DB.execute "SELECT name FROM PRAGMA_TABLE_INFO('publications')").flatten
    p "All publications:"
    p header
    (DB.execute "SELECT * FROM publications").each { |publication| p publication }
    p "---"
    p "Publication by ISBN (`5554-5545-4518`)"
    p header
    p (DB.execute "SELECT * FROM publications WHERE isbn = (?)" , "5554-5545-4518").first
    p "---"
    p "Publication by author e-mail (`null-lieblich@echocat.org`)"
    p header
    author_id = DB.execute "SELECT id FROM authors WHERE email = (?)", "null-lieblich@echocat.org"
    (DB.execute "SELECT * FROM publications p LEFT JOIN author_publication ap ON ap.publication_id = p.id WHERE ap.author_id = (?)", author_id).each  { |publication| p publication }
    p "---"
    p "All publications sorted by `title`:"
    p header
    (DB.execute "SELECT * FROM publications ORDER BY title").each { |publication| p publication }
    p "---"
    Echocat::CleanupJob.new.()
  end
end
