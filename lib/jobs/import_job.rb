require "csv"
require "sqlite3"

module Echocat
  class ImportJob
    DB = SQLite3::Database.new "kata.db"

    def call
      create_tables
      insert_authors
      insert_publications
    end

    private

    def create_tables
      DB.execute <<-SQL
        CREATE TABLE IF NOT EXISTS authors (
          id        INTEGER PRIMARY KEY AUTOINCREMENT,
          email     TEXT UNIQUE NOT NULL,
          firstname TEXT,
          lastname  TEXT
        )
      SQL
      DB.execute <<-SQL
        CREATE TABLE IF NOT EXISTS publications (
          id           INTEGER PRIMARY KEY AUTOINCREMENT,
          title        TEXT NOT NULL,
          isbn         TEXT UNIQUE NOT NULL,
          description  TEXT,
          published_at DATE
        )
      SQL
      DB.execute <<-SQL
        CREATE TABLE IF NOT EXISTS author_publication (
          author_id       INTEGER NOT NULL,
          publication_id  INTEGER NOT NULL,
          UNIQUE (author_id, publication_id),
          FOREIGN KEY (author_id)      REFERENCES author (id),
          FOREIGN KEY (publication_id) REFERENCES publication(id)
        )
      SQL
    end

    def insert_authors
      CSV.foreach("data/authors.csv", col_sep: ";", headers: true, header_converters: :symbol) do |row|
        DB.execute "INSERT INTO authors (email, firstname, lastname) VALUES (?, ?, ?)", row.fields
      end
    end

    def insert_publications
      CSV.foreach("data/books.csv", col_sep: ";", headers: true, header_converters: :symbol) do |row|
        DB.execute "INSERT INTO publications (title, isbn, description) VALUES (?, ?, ?)", [row[:title], row[:isbn], row[:description]]
        insert_to_linking_table(row)
      end
      CSV.foreach("data/magazines.csv", col_sep: ";", headers: true, header_converters: :symbol) do |row|
        DB.execute "INSERT INTO publications (title, isbn, published_at) VALUES (?, ?, ?)", [row[:title], row[:isbn], row[:publishedat]]
        insert_to_linking_table(row)
       end
    end

    def insert_to_linking_table(row)
      publication_id = DB.last_insert_row_id
      placeholders = (['?'] * row[:authors].split(',').length).join(',')
      author_ids = DB.execute "SELECT id FROM authors WHERE email IN (#{placeholders})", row[:authors].split(',')
      author_ids.flatten.each do |author_id|
        DB.execute "INSERT INTO author_publication(author_id, publication_id) VALUES (?, ?)", [author_id, publication_id]
      end
    end
  end
end
