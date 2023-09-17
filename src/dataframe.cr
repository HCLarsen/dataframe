require "csv"

require "./column"
require "./row"
require "./parser/*"

class Dataframe
  VERSION = "0.1.0"

  @column_defs = Hash(String, ColumnType).new
  getter data = Array(Array(Type)).new

  # Creates an empty `Dataframe` instance, with no columns or rows.
  def initialize
  end

  # Creates a new `Dataframe` instance with the specified headers, and columns
  # of type `String`, but with no data.
  def initialize(column_names : Array(String))
    types = Array(ColumnType).new(column_names.size, String)
    @column_defs = Hash.zip(column_names, types)
  end

  # Creates a new `Dataframe` instance with column names and types defined by *columns*, but
  # with no data.
  def initialize(columns)
    @column_defs.merge!(columns)
  end

  # Creates a new `Dataframe` instance with the given headers and rows.
  #
  # Raises an InvalidDataframe error if the headers and each row don't all have
  # the same length.
  def initialize(headers : Array(String), rows : Array(Array(Type)))
    width = headers.size
    if rows.any? { |e| e.size != width }
      raise InvalidDataframeError.new
    end

    columns_data = rows.transpose

    headers.each_with_index do |header, index|
      column = columns_data[index].compact
      if column.size > 0
        @column_defs[header] = column.first.class
      else
        raise InvalidDataframeError.new("Can't determine type of column \"#{header}\".")
      end
    end

    rows.each do |row|
      if row.size == width
        add_row(row)
      end
    end
  end

  def self.from_csv(string_or_io : String | IO, headers : Bool = true, separator : Char = CSVLexer::DEFAULT_SEPARATOR, quote_char : Char = CSVLexer::DEFAULT_QUOTE_CHAR) : Dataframe
    CSVParser.new(string_or_io, headers, separator, quote_char).parse
  end

  # Returns the number of rows in the `Dataframe`.
  def row_count
    @data.size
  end

  # Append. Alias for `add_row`.
  def <<(row : Array(Type))
    add_row(row)
  end

  # Append. Alias for `add_row`.
  def <<(row : Row)
    add_row(row)
  end

  # Append a new row to the bottom of `self`.
  def add_row(row : Array(Type))
    if row.size != headers.size
      raise InvalidRowError.new("Row has different size than Dataframe")
    end

    types = @column_defs.values

    row.each_with_index do |element, index|
      if !element.nil? && element.class != types[index]
        raise InvalidRowError.new("Invalid type for column \"#{headers[index]}\". Expected (#{types[index]} | Nil), but got #{element.class}")
      end
    end

    @data.push(row)
  end

  # Append a new row to the bottom of `self`.
  def add_row(row : Row)
    row.headers.each do |header|
      if !headers.includes?(header)
        raise KeyError.new("Missing header: #{header}")
      end
    end

    new_row = Row.new
    @column_defs.each do |key, value|
      row_value_for_header = row[key]?
      if row_value_for_header.nil? || value == row_value_for_header.class
        new_row[key] = row_value_for_header
      else
        raise InvalidRowError.new("Invalid type for column \"#{key}\". Expected (#{value} | Nil), but got #{row_value_for_header.class}")
      end
    end

    @data.push(new_row.to_a)
  end

  # Iterates over the rows of `self`.
  def each(& : Array(Type) ->) : Nil
    @data.each do |row|
      yield row
    end
  end

  # Iterates over the rows of `self`, returning each row as an instance of `Row`.
  def each_row(& : Row ->) : Nil
    @data.each do |row|
      yield Row.new(row, headers)
    end
  end

  # Returns the column names as an `Array`.
  def headers : Array(String)
    @column_defs.keys
  end

  # Returns the columns of `self` as a `Hash` with the headers as keys, and `Column`s as values.
  def columns : Hash(String, Column(String) | Column(Int32) | Column(Float64) | Column(Bool))
    output = {} of String => (Column(String) | Column(Int32) | Column(Float64) | Column(Bool))
    data_columns = @data.transpose

    @column_defs.each_with_index do |key_value, index|
      column_header = key_value[0]
      column_type = key_value[1]
      column_data = data_columns[index]
      output[column_header] = create_column(column_type, column_data)
    end

    output
  end

  def [](header : String) : Dataframe::Column
    data_columns = @data.transpose
    index = headers.index(header)
    column_type = @column_defs[header]

    if index
      create_column(column_type, data_columns[index])
    else
      raise KeyError.new("Missing header: \"#{header}\"")
    end
  end

  def []=(header : String, new_column : Column) : self
    if headers.includes?(header)
      column_index = headers.index!(header)

      @data.map_with_index! do |data_row, index|
        data_row[column_index] = new_column[index]

        data_row
      end
    else
      add_column(header, new_column.to_a)
    end
    self
  end

  # Adds a new empty column to `self`.
  def add_column(header : String, type : ColumnType = String)
    @column_defs[header] = type

    @data.map! do |data_row|
      data_row.push(nil)
    end
  end

  # Adds a new column to `self`, with content specified by *data*.
  #
  # The type is determined by the content of *data*.
  #
  # **NOTE**: If *data* doesn't have any non-null values, a runtime error will occur.
  def add_column(header : String, data : Array(Type))
    if data.size != @data.size
      raise InvalidDataframeError.new("New column must be same size as other columns: #{@data.size}")
    end

    @column_defs[header] = data.compact.first.class

    @data.map_with_index! do |data_row, index|
      data_row.push(data[index])
    end
  end

  # def modify_column(header : String, &) : self
  #   columns_data = @data.transpose
  #   index = headers.index(header)
  #   # type = @column_defs[header]

  #   if index.nil?
  #     raise KeyError.new("Missing header: \"#{header}\"")
  #   end

  #   # column_data = columns_data[index]
  #   # # puts column_data.class

  #   # column = columns[header]
  #   # puts column.class

  #   # if column_data.is_a?(Array(String))
  #   #   column_data.map! { |cell| yield cell.as(String) }
  #   # elsif column_data.is_a?(Array(Int32))
  #   #   column_data.map! { |cell| yield cell.as(Int32) }
  #   # elsif column_data.is_a?(Array(Float64))
  #   #   column_data.map! { |cell| yield cell.as(Float64) }
  #   # elsif column_data.is_a?(Array(Bool))
  #   #   column_data.map! { |cell| yield cell.as(Bool) }
  #   # end

  #   # columns_data[index] = column_data
  #   # @data = columns_data.transpose

  #   column = columns[header]
  #   # puts column.class

  #   if column.is_a?(Dataframe::Column(String))
  #     puts "String: #{typeof(column)}"
  #     # new_column = column.to_a.map { |cell| yield cell.as(String) }
  #   elsif column.is_a?(Dataframe::Column(Int32))
  #     puts "Int32: #{typeof(column)}"
  #     # new_column = column.to_a.map { |cell| yield cell.as(Int32) }
  #   elsif column.is_a?(Dataframe::Column(Float64))
  #     puts "Float64: #{typeof(column)}"
  #     # new_column = column.to_a.map { |cell| yield cell.as(Float64) }
  #     # column_data.map! { |cell| yield cell.as(Float64) }
  #   elsif column.is_a?(Dataframe::Column(Bool))
  #     puts "Bool: #{typeof(column)}"
  #     # new_column = column.to_a.map { |cell| yield cell.as(Bool) }
  #     # column_data.map! { |cell| yield cell.as(Bool) }
  #   else
  #     raise InvalidTypeError.new(column.class)
  #   end

  #   # puts new_column.class
  #   # puts (Array(Type).new + new_column).class

  #   # columns_data[index] = (Array(Type).new + new_column)
  #   # @data = columns_data.transpose

  #   self
  # end

  # Changes the header of the specified column to a new value.
  #
  # Makes no changes if *old_header* isn't a header.
  def rename_column(old_header, new_header)
    column_names = @column_defs.keys
    types = @column_defs.values
    if index = column_names.index(old_header)
      column_names[index] = new_header
    end
    @column_defs = Hash.zip(column_names, types)
  end

  # Returns a new `Dataframe` without the given columns.
  def reject_columns(headers : Array(String)) : Dataframe
    new_columns = columns.reject(headers)

    new_rows = new_columns.values.map(&.to_a).transpose

    Dataframe.new(new_columns.keys, new_rows)
  end

  # Removes a list of columns.
  def reject_columns!(headers : Array(String)) : self
    new_columns = columns.reject(headers)
    @data = new_columns.values.map(&.to_a).transpose

    @column_defs.reject!(headers)

    self
  end

  # Returns a new `Dataframe` with the given columns.
  def select_columns(headers : Array(String)) : Dataframe
    new_columns = columns.select(headers)
    new_rows = new_columns.values.map(&.to_a).transpose

    Dataframe.new(headers, new_rows)
  end

  # Removes every column except the given ones.
  def select_columns!(headers : Array(String)) : self
    new_columns = columns.select(headers)
    @data = new_columns.values.map(&.to_a).transpose

    @column_defs.select!(headers)

    self
  end

  # Returns a new `Dataframe` with columns ordered by *new_headers*.
  #
  # **NOTE**: Any column with names omitted from *new_headers* will not be included
  # in the new `Dataframe`.
  #
  # See also: `Dataframe#select`.
  def order_columns(new_headers : Array(String)) : Dataframe
    old_columns = columns
    new_column_data = Hash(String, Column(String) | Column(Int32) | Column(Float64) | Column(Bool)).new

    new_headers.each do |header|
      new_column_data[header] = columns[header]
    end

    new_data = new_column_data.values.map(&.to_a).transpose

    Dataframe.new(new_headers, new_data)
  end

  # Modifies `self` by rearranging columns in order specified by *new_headers*.
  #
  # **NOTE**: Any column with names omitted from *new_headers* will be removed from
  # `self`.
  #
  # See also: `Dataframe#select!`.
  def order_columns!(new_headers : Array(String)) : self
    old_columns = columns
    new_column_defs = Hash(String, ColumnType).new
    new_column_data = Hash(String, Column(String) | Column(Int32) | Column(Float64) | Column(Bool)).new

    new_headers.each do |header|
      new_column_defs[header] = @column_defs[header]
      new_column_data[header] = columns[header]
    end

    @column_defs = new_column_defs
    @data = new_column_data.values.map(&.to_a).transpose

    self
  end

  # Returns the data of the `Dataframe` as an array of `Row`.
  def rows : Array(Row)
    @data.map do |data_row|
      Row.new(data_row, headers)
    end
  end

  # Returns a `Dataframe` with all the elements in the collection for which
  # the passed block is falsey.
  def reject(& : Row ->) : Dataframe
    new_rows = Array(Array(Type)).new
    each_row { |e| new_rows << e.to_a unless yield e }

    Dataframe.new(headers, new_rows)
  end

  # Modifies `self`, deleting the rows in the collection for which the
  # passed block is truthy. Returns `self`.
  #
  # See also: `Dataframe#reject`.
  def reject!(& : Row ->) : self
    new_rows = rows

    new_rows.reject! { |e| yield e }
    @data = new_rows.map { |e| e.to_a }

    self
  end

  # Returns a new `Dataframe` with only rows for which the passed block is truthy.
  def select(& : Row ->) : Dataframe
    new_rows = Array(Array(Type)).new
    each_row { |e| new_rows << e.to_a if yield e }

    Dataframe.new(headers, new_rows)
  end

  # Returns a new `Dataframe` with only rows for which the passed block is truthy.
  def select!(& : Row ->) : self
    new_rows = rows

    new_rows.select! { |e| yield e }
    @data = new_rows.map { |e| e.to_a }

    self
  end

  def sort_by(& : Row ->) : Dataframe
    new_rows = rows.sort_by { |row| yield row }.map { |e| e.to_a }

    Dataframe.new(headers, new_rows)
  end

  def sort_by!(& : Row ->) : self
    new_rows = rows.sort_by { |row| yield row }
    @data = new_rows.map { |e| e.to_a }

    self
  end

  def sort_by(column : String, desc = false) : Dataframe
    new_rows = rows.sort do |row1, row2|
      cell1 = row1[column]
      cell2 = row2[column]

      if cell1.nil? || cell2.nil?
        1
      elsif cell1.is_a?(Int32)
        cell1 <=> cell2.as(Int32)
      elsif cell1.is_a?(Float64)
        cell1 <=> cell2.as(Float64)
      else
        cell1.as(String) <=> cell2.as(String)
      end
    end

    if desc
      new_rows.reverse!
    end

    Dataframe.new(headers, new_rows.map { |e| e.to_a })
  end

  def sort_by!(column : String, desc = false) : self
    column_index = headers.index!(column)

    @data.sort! do |row1, row2|
      cell1 = row1[column_index]
      cell2 = row2[column_index]

      if cell1.nil? || cell2.nil?
        1
      elsif cell1.is_a?(Int32)
        cell1 <=> cell2.as(Int32)
      elsif cell1.is_a?(Float64)
        cell1 <=> cell2.as(Float64)
      else
        cell1.as(String) <=> cell2.as(String)
      end
    end

    if desc
      @data.reverse!
    end

    self
  end

  # Returns a `Tuple` of the dataframe's dimensions in the form of
  # { rows, columns }
  def shape : Tuple(Int32, Int32)
    {@data.size, @column_defs.keys.size}
  end

  private def create_column(type : Class, data : Array(Type))
    if type == String
      Dataframe::Column(String).new(data)
    elsif type == Int32
      Dataframe::Column(Int32).new(data)
    elsif type == Float64
      Dataframe::Column(Float64).new(data)
    elsif type == Bool
      Dataframe::Column(Bool).new(data)
    else
      raise InvalidTypeError.new(type)
    end
  end

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

  # Returns a new `Dataframe` that is the result of a right outer join of the
  # receiver and *other*, using the headers in *on* to match rows.
  # def right_outer_join(other : Dataframe, on : Array(String)) : Dataframe
  #   other.left_outer_join(self, on: on)
  # end

  # def select_rows(& : Row ->) : Dataframe
  #   # new_rows = @data.select { |e| yield e }
  #   new_rows = Array(Array(Type)).new
  #   each_row { |e| new_rows << e if yield e }

  #   Dataframe.new(@column_defs, new_rows)
  # end

  # def select_rows!(& : Array(String) ->) : Nil
  #   @data.select! { |e| yield e }
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
