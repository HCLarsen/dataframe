require "csv"

require "./row"

class Dataframe
  VERSION = "0.1.0"

  alias Type = String | Int32 | Float64 | Bool | Nil
  alias ColumnType = String.class | Int32.class | Float64.class | Bool.class

  # Raised when an error is encountered during parsing.
  class InvalidDataframeError < Exception
    def initialize(message = "Rows are of uneven length")
      super(message)
    end
  end

  class InvalidRowError < Exception
    def initialize(message = "Row is an invalid match for Dataframe")
      super(message)
    end
  end

  @columns = Hash(String, ColumnType).new
  getter data = Array(Array(Type)).new

  # Creates an empty `Dataframe` instance, with no columns or rows.
  def initialize
  end

  # Creates a new `Dataframe` instance with the specified headers, and columns
  # of type `String`.
  def initialize(headers : Array(String))
    types = Array(ColumnType).new(headers.size, String)
    @columns = Hash.zip(headers, types)
  end

  def initialize(columns)
    @columns.merge!(columns)
  end

  # Creates a new `Dataframe` instance with the given headers and rows.
  #
  # Raises an InvalidDataframe error if the headers and each row don't all have
  # the same length.
  def initialize(headers : Array(String), rows : Array(Array(Type)))
    first_row = rows[0]
    headers.each_with_index do |header, index|
      cell = first_row[index]
      if !cell.nil?
        @columns[header] = cell.class
      end
    end

    width = headers.size
    rows.each do |row|
      if row.size == width
        add_row(row)
      else
        raise InvalidDataframeError.new
      end
    end
  end

  def <<(row : Array(Type))
    add_row(row)
  end

  def <<(row : Row)
    add_row(row)
  end

  def add_row(row : Array(Type))
    if row.size != headers.size
      raise InvalidRowError.new("Row has different size than Dataframe")
    end

    @data.push(row)
  end

  def add_row(row : Row)
    row.headers.each do |header|
      if !headers.includes?(header)
        raise InvalidRowError.new("Row has values not in Dataframe")
      end
    end

    new_row = Row.new
    headers.each do |header|
      new_row[header] = row[header]?
    end

    @data.push(new_row.to_a)
  end

  # Returns the column names as an `Array`.
  def headers : Array(String)
    @columns.keys
  end

  # Returns the data of the `Dataframe` as an array of `Row`.
  def rows : Array(Row)
    @data.map do |data_row|
      Row.new(data_row, headers)
    end
  end

  # Returns a `Tuple` of the dataframe's dimensions in the form of { rows, columns }
  def shape : Tuple(Int32, Int32)
    {@data.size, @columns.keys.size}
  end

  # def columns : Hash(String, Type)
  #   output = {} of String => Type
  #   data_columns = @data.transpose

  #   @columns.each do |key, value|
  #     output[key]
  #   end

  #   output
  # end

  # Creates a new `Dataframe` instance from a CSV string, treating the first
  # row as the header row.
  #
  # Raises an InvalidDataframeError if all lines of the CSV aren't the same
  # length
  # def self.from_csv(csv : String) : Dataframe
  #   rows = Array(Array(String)).new
  #   CSV.each_row(csv) do |row|
  #     rows << row.map { |e| e.strip }
  #   end

  #   headers = rows.shift

  #   Dataframe.new(headers, rows)
  # end

  # Creates a new `Dataframe` instance from a CSV file, treating the first
  # row as the header row.
  #
  # Raises an InvalidDataframeError if all lines of the CSV aren't the same
  # length
  # def self.from_csv_file(filename : String) : Dataframe
  #   file = File.read(filename)

  #   self.from_csv(file)
  # end

  # Returns a `Hash` where the keys are the headers of the `Dataframe` instance,
  # and the values are arrays of the corresponding column.
  # def columns : Hash(String, Array(String))
  #   columns = Hash(String, Array(String)).new

  #   @data.transpose.each_with_index do |column, index|
  #     columns[@headers[index]] = column
  #   end

  #   columns
  # end

  # Returns a new dataframe with all duplicate rows from the reciever, based
  # on the headers specified.
  #
  # Passing in `true` as the second argument modifies `self` by removing the
  # same duplicate rows.
  #
  # This method differs from `#remove_duplicates` in that it removes all rows
  # that have duplicates in the dataframe, including the first.
  #
  # If *headers* is left blank, it calculates duplicates based on all columns
  #
  # ```
  # dataframe.rows # => [["Jim", "41", "Hawkins, Indiana, USA"],["Eddie", "20", "Hawkins, Indiana, USA"],["Jim", "41", "Siberia, USSR"],["Yuri", "47", "Siberia, USSR"]]
  #
  # duplicates = dataframe.duplicates(["Name"], true)
  #
  # duplicates.rows # => [["Jim", "44", "Hawkins, Indiana, USA"],["Jim", "44", "Siberia, USSR"]]
  # dataframe.rows  # => [["Eddie", "20", "Hawkins, Indiana, USA"],["Yuri", "47", "Siberia, USSR"]]
  # ```
  #
  # def duplicates(headers = @headers, remove = false) : Dataframe
  #   header_indexes = header_indexes(headers)

  #   hash = Hash(String, Array(String)).new
  #   indexes = Hash(String, Int32).new(0)
  #   duplicate_rows = Array(Array(String)).new
  #   new_rows = Array(Array(String)).new

  #   @data.each do |row|
  #     values = header_indexes.map { |i| row[i] }
  #     indexes[values.join] += 1
  #   end

  #   indexes.select! { |k, v| v > 1 }
  #   set = Set.new(indexes.keys)

  #   @data.each do |row|
  #     values = header_indexes.map { |i| row[i] }
  #     index = values.join

  #     if set.includes?(index)
  #       duplicate_rows << row
  #     elsif remove
  #       new_rows << row
  #     end
  #   end

  #   if remove
  #     @data = new_rows
  #   end

  #   Dataframe.new(self.headers, duplicate_rows)
  # end

  # def each(& : Array(String) ->) : Nil
  #   @data.each do |row|
  #     yield row
  #   end
  # end

  # Returns a new `Dataframe` that is the result of a full join of the
  # receiver and *other*, using the headers in *on* to match rows.
  # def full_join(other : Dataframe, on : Array(String)) : Dataframe
  #   new_headers = (@headers + other.headers).uniq
  #   new_rows = Array(Array(String)).new

  #   indexed = indexed_by(on)
  #   indexed_other = other.indexed_by(on)

  #   indexed.each do |index, row|
  #     new_row = Array(String).new(new_headers.size, "")
  #     new_hash = Hash.zip(new_headers, new_row)

  #     new_hash.merge!(Hash.zip(@headers, row))
  #     if match = indexed_other[index]?
  #       new_hash.merge!(Hash.zip(other.headers, match))
  #       indexed_other.delete(index)
  #     end
  #     new_rows << new_hash.values
  #   end

  #   indexed_other.each do |index, row|
  #     new_row = Array(String).new(new_headers.size, "")
  #     new_hash = Hash.zip(new_headers, new_row)
  #     new_hash.merge!(Hash.zip(other.headers, row))

  #     new_rows << new_hash.values
  #   end

  #   Dataframe.new(new_headers, new_rows)
  # end

  # def indexed_by(headers : Array(String)) : Hash(String, Array(String))
  #   header_indexes = header_indexes(headers)

  #   hash = Hash(String, Array(String)).new

  #   @data.each do |row|
  #     values = header_indexes.map { |i| row[i] }
  #     index = values.join
  #     hash[index] = row
  #   end

  #   hash
  # end

  # Returns a new `Dataframe` that is the result of an inner join of the
  # receiver and *other*, using the headers in *on* to match rows.
  # def inner_join(other : Dataframe, on : Array(String)) : Dataframe
  #   new_headers = (@headers + other.headers).uniq
  #   new_rows = Array(Array(String)).new

  #   indexed = indexed_by(on)
  #   indexed_other = other.indexed_by(on)

  #   indexed.each do |index, row|
  #     if match = indexed_other[index]?
  #       new_row = Array(String).new(new_headers.size, "")
  #       new_hash = Hash.zip(new_headers, new_row)

  #       new_hash.merge!(Hash.zip(@headers, row))
  #       new_hash.merge!(Hash.zip(other.headers, match))
  #       new_rows << new_hash.values
  #     end
  #   end

  #   Dataframe.new(new_headers, new_rows)
  # end

  # Returns a new `Dataframe` that is the result of a left outer join of the
  # receiver and *other*, using the headers in *on* to match rows.
  # def left_outer_join(other : Dataframe, on : Array(String)) : Dataframe
  #   new_headers = (@headers + other.headers).uniq
  #   new_rows = Array(Array(String)).new

  #   indexed = indexed_by(on)
  #   indexed_other = other.indexed_by(on)

  #   indexed.each do |index, row|
  #     new_row = Array(String).new(new_headers.size, "")
  #     new_hash = Hash.zip(new_headers, new_row)

  #     new_hash.merge!(Hash.zip(@headers, row))
  #     if match = indexed_other[index]?
  #       new_hash.merge!(Hash.zip(other.headers, match))
  #     end
  #     new_rows << new_hash.values
  #   end

  #   Dataframe.new(new_headers, new_rows)
  # end

  # Iterates through all elements in the column specified by *header*, running
  # the provided block on each element.
  # def modify_column(header : String, & : String ->)
  #   new_columns = columns

  #   new_column = new_columns[header].map do |element|
  #     yield element
  #   end

  #   new_columns[header] = new_column
  #   @data = new_columns.values.transpose
  # end

  # Return a new `Dataframe` without the specified columns.
  # def remove_columns(headers : Array(String)) : Dataframe
  #   new_columns = columns.reject(headers)

  #   new_rows = new_columns.values.transpose

  #   Dataframe.new(new_columns.keys, new_rows)
  # end

  # Removes all rows for which a previous row is identical in the columns
  # specified by *headers*.
  #
  # If *headers* is left blank, it calculates duplicates based on all columns
  # def remove_duplicates(headers = [] of String)
  #   new_rows = Array(Array(String)).new

  #   if headers.size > 0
  #     header_indexes = header_indexes(headers)

  #     indexes = [] of String

  #     @data.each do |row|
  #       values = header_indexes.map { |i| row[i] }
  #       index = values.join
  #       unless indexes.includes?(index)
  #         indexes << index
  #         new_rows << row
  #       end
  #     end
  #   else
  #     new_rows = @data.uniq
  #   end

  #   @data = new_rows
  # end

  # Changes the header of the specified column to a new value.
  #
  # Makes no changes if *old_header* isn't a header.
  # def rename_column(old_header, new_header)
  #   if index = @headers.index(old_header)
  #     @headers[index] = new_header
  #   end
  # end

  # Returns a new `Dataframe` that is the result of a right outer join of the
  # receiver and *other*, using the headers in *on* to match rows.
  # def right_outer_join(other : Dataframe, on : Array(String)) : Dataframe
  #   other.left_outer_join(self, on: on)
  # end

  # Returns the number of rows in the `Dataframe`.
  def row_count
    @data.size
  end

  # def select_rows(& : Array(String) ->) : Dataframe
  #   new_rows = @data.select { |e| yield e }

  #   Dataframe.new(@headers, new_rows)
  # end

  # def select_rows!(& : Array(String) ->) : Nil
  #   @data.select! { |e| yield e }
  # end

  # Return a new `Dataframe` with only the specified columns.
  # def select_columns(headers : Array(String)) : Dataframe
  #   new_columns = columns.select(headers)

  #   new_rows = new_columns.values.transpose

  #   Dataframe.new(headers, new_rows)
  # end

  # Outputs the `Dataframe` instance as a string in CSV format.
  # def to_csv
  #   output = @headers.map { |e| %("#{e}") }.join(",") + "\n"
  #   return output + @data.map { |row| row.map { |e| %("#{e}") }.join(",") + "\n" }.join
  # end

  # Outputs the `Dataframe` instance in an easy to read table format.
  # def to_table(range = (0..-1)) : String
  #   headers_and_rows = [@headers] + @data

  #   column_widths = headers_and_rows.transpose.map do |column|
  #     column.max_of &.size
  #   end

  #   table = @headers.map_with_index { |e, i| pad_cell(e, column_widths[i]) }.join("  ")

  #   @data[range].each do |row|
  #     line = row.map_with_index { |e, i| pad_cell(e, column_widths[i]) }.join("  ")
  #     table += "\n" + line
  #   end

  #   table
  # end

  # private def pad_cell(value : String, length) : String
  #   string_length = value.size
  #   difference = length - string_length
  #   difference = 0 if difference < 0

  #   padding = " " * difference

  #   return value + padding
  # end

  # private def header_indexes(headers : Array(String)) : Array(Int32)
  #   headers.map { |header| @headers.index(header) }.compact
  # end
end
