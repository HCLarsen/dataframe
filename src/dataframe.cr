require "csv"

class Dataframe
  VERSION = "0.1.0"

  getter headers : Array(String)
  getter rows : Array(Array(String))

  def initialize(@headers, @rows)
  end

  def self.from_csv_file(filename : String) : Dataframe
    file = File.read(filename)

    rows = Array(Array(String)).new
    CSV.each_row(file) do |row|
      rows << row
    end

    headers = rows.shift

    Dataframe.new(headers, rows)
  end

  def to_csv
    output = @headers.map{ |e| %("#{e}") }.join(",") + "\n"
    return output + @rows.map { |row| row.map{ |e| %("#{e}") }.join(",") + "\n" }.join
  end

  def columns : Hash(String, Array(String))
    columns = Hash(String, Array(String)).new

    @rows.transpose.each_with_index do |column, index|
      columns[@headers[index]] = column
    end

    columns
  end

  def inner_join(other : Dataframe, on : Array(String)) : Dataframe
    new_headers = (@headers + other.headers).uniq
    new_rows = Array(Array(String)).new

    indexed = indexed_by(on)
    indexed_other = other.indexed_by(on)

    indexed.each do |index, row|
      if match = indexed_other[index]?
        new_row = Array(String).new(new_headers.size, "")
        new_hash = Hash.zip(new_headers, new_row)

        new_hash.merge!(Hash.zip(@headers, row))
        new_hash.merge!(Hash.zip(other.headers, match))
        new_rows << new_hash.values
      end
    end

    Dataframe.new(new_headers, new_rows)
  end

  def left_outer_join(other : Dataframe, on : Array(String)) : Dataframe
    new_headers = (@headers + other.headers).uniq
    new_rows = Array(Array(String)).new

    indexed = indexed_by(on)
    indexed_other = other.indexed_by(on)

    indexed.each do |index, row|
      new_row = Array(String).new(new_headers.size, "")
      new_hash = Hash.zip(new_headers, new_row)

      new_hash.merge!(Hash.zip(@headers, row))
      if match = indexed_other[index]?
        new_hash.merge!(Hash.zip(other.headers, match))
      end
      new_rows << new_hash.values
    end

    Dataframe.new(new_headers, new_rows)
  end

  def right_outer_join(other : Dataframe, on : Array(String)) : Dataframe
    other.left_outer_join(self, on: on)
  end

  def full_join(other : Dataframe, on : Array(String)) : Dataframe
    new_headers = (@headers + other.headers).uniq
    new_rows = Array(Array(String)).new

    indexed = indexed_by(on)
    indexed_other = other.indexed_by(on)

    indexed.each do |index, row|
      new_row = Array(String).new(new_headers.size, "")
      new_hash = Hash.zip(new_headers, new_row)

      new_hash.merge!(Hash.zip(@headers, row))
      if match = indexed_other[index]?
        new_hash.merge!(Hash.zip(other.headers, match))
        indexed_other.delete(index)
      end
      new_rows << new_hash.values
    end

    indexed_other.each do |index, row|
      new_row = Array(String).new(new_headers.size, "")
      new_hash = Hash.zip(new_headers, new_row)
      new_hash.merge!(Hash.zip(other.headers, row))

      new_rows << new_hash.values
    end

    Dataframe.new(new_headers, new_rows)
  end

  def indexed_by(headers : Array(String)) : Hash(String, Array(String))
    indexes = headers.map { |header| @headers.index(header) }.compact

    hash = Hash(String, Array(String)).new

    @rows.each do |row|
      values = indexes.map { |i| row[i] }
      index = values.join
      hash[index] = row
    end

    hash
  end

  def modify_column(header : String, & : String ->)
    new_columns = columns

    new_column = new_columns[header].map do |element|
      yield element
    end

    new_columns[header] = new_column
    @rows = new_columns.values.transpose
  end

  def remove_duplicates(headers : Array(String))
    header_indexes = headers.map { |header| @headers.index(header) }.compact

    indexes = [] of String
    new_rows = Array(Array(String)).new

    @rows.each do |row|
      values = header_indexes.map { |i| row[i] }
      index = values.join
      unless indexes.includes?(index)
        indexes << index
        new_rows << row
      end
    end

    @rows = new_rows
  end

  def duplicates(headers : Array(String), remove = false) : Dataframe
    header_indexes = headers.map { |header| @headers.index(header) }.compact

    hash = Hash(String, Array(String)).new
    indexes = Hash(String, Int32).new(0)
    duplicate_rows = Array(Array(String)).new
    new_rows = Array(Array(String)).new

    @rows.each do |row|
      values = header_indexes.map { |i| row[i] }
      indexes[values.join] += 1
    end

    indexes.select! { |k, v| v > 1 }
    set = Set.new(indexes.keys)

    @rows.each do |row|
      values = header_indexes.map { |i| row[i] }
      index = values.join

      if set.includes?(index)
        duplicate_rows << row
      elsif remove
        new_rows << row
      end
    end

    if remove
      @rows = new_rows
    end

    Dataframe.new(self.headers, duplicate_rows)
  end
end
