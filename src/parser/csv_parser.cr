require "./csv_lexer"

require "../dataframe"

# **NOTE** The CSV Spec(RFC 4180) does state that leading and trailing whitespaces
# should not be ignored, some implementations of CSV do still use them. For this reason,
# `Dataframe` will ignore leading and trailing whitespace in unquoted cells.
class Dataframe
  class CSVParser
    @headers : Bool
    @row_number = 0
    @column_number = 0

    def initialize(string_or_io : String | IO, @headers = true, separator : Char = CSVLexer::DEFAULT_SEPARATOR, quote_char : Char = CSVLexer::DEFAULT_QUOTE_CHAR)
      @lexer = CSVLexer.new(string_or_io, separator, quote_char)
      @column_types = [] of ColumnType | Nil.class
      @column_names = [] of String
    end

    def parse : Dataframe
      data = Array(Array(Type)).new
      dataframe_width = 0

      if @headers
        @column_names = parse_header_row
        dataframe_width = @column_names.size

        first_row = parse_data_row
        @column_types = first_row.map { |e| e.class }
        data << first_row
      else
        first_row = parse_data_row
        @column_types = first_row.map { |e| e.class }
        data << first_row

        dataframe_width = data[0].size
        @column_names = (0...dataframe_width).map { |e| "c#{e}" }
      end

      return Dataframe.new if dataframe_width == 0

      while true
        new_row = parse_data_row
        if new_row.size == dataframe_width
          data << new_row
        elsif new_row.size == 0
          break
        else
          raise Dataframe::InvalidDataframeError.new("Row #{@row_number} has unequal size to other rows")
        end
      end

      Dataframe.new(@column_names, data)
    end

    private def parse_header_row : Array(String)
      header_row = [] of String

      while true
        token = @lexer.next_token
        if token.is_cell?
          header_row << token.string_value
        else
          break
        end
      end

      header_row
    end

    private def parse_data_row : Array(Type)
      cell : Type
      row = [] of Type

      @row_number += 1
      @column_number = 1

      while true
        expected_type = @column_types[@column_number - 1]?
        token = @lexer.next_token
        case token.kind
        when CSVLexer::Token::Kind::String
          cell = token.string_value
        when CSVLexer::Token::Kind::Int
          cell = token.int_value
        when CSVLexer::Token::Kind::Float
          cell = token.float_value
        when CSVLexer::Token::Kind::Null
          cell = nil
        when CSVLexer::Token::Kind::Newline, CSVLexer::Token::Kind::EOF
          break
        else
          raise CSV::MalformedCSVError.new("Unexpected type '#{token.kind}'", token.line_number, token.column_number)
        end

        if expected_type && cell.class != Nil && expected_type != cell.class
          raise Dataframe::InvalidTypeError.new("Unexpected #{cell.class} in #{@column_names[@column_number - 1]} column, row #{@row_number}. Column type is Int")
        end

        @column_number += 1
        row << cell
      end

      return row
    end

    # private def raise(msg)
    #   ::raise CSV::MalformedCSVError.new(msg, @lexer.line_number, @lexer.column_number)
    # end
  end
end
