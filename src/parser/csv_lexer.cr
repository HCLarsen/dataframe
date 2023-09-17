require "csv"
require "json"

class Dataframe
  class CSVLexer
    DEFAULT_SEPARATOR  = ','
    DEFAULT_QUOTE_CHAR = '"'

    class Token
      enum Kind
        Null
        False
        True
        Int
        Float
        String
        Newline
        EOF
      end

      property kind : Kind
      property line_number : Int32
      property column_number : Int32
      property raw_value : String
      property string_value : String

      def initialize
        @kind = :EOF
        @line_number = 0
        @column_number = 0
        @string_value = ""
        @raw_value = ""
      end

      def is_cell? : Bool
        @kind != Kind::Newline && @kind != Kind::EOF
      end

      def int_value : Int32
        raw_value.to_i
      rescue exc : ArgumentError
        raise JSON::ParseException.new(exc.message, line_number, column_number)
      end

      def float_value : Float64
        raw_value.to_f64
      rescue exc : ArgumentError
        raise JSON::ParseException.new(exc.message, line_number, column_number)
      end
    end

    # Returns the current `Token`.
    getter token : Token = Token.new
    private getter current_char
    private getter last_char : Char?

    def initialize(string : String, @separator : Char = DEFAULT_SEPARATOR, @quote_char : Char = DEFAULT_QUOTE_CHAR)
      @io = IO::Memory.new(string)
      @buffer = IO::Memory.new
      @column_number = 1
      @line_number = 1
      @last_char = nil

      @current_char = @io.read_char || '\0'
    end

    def initialize(@io : IO, @separator : Char = DEFAULT_SEPARATOR, @quote_char : Char = DEFAULT_QUOTE_CHAR)
      @buffer = IO::Memory.new
      @column_number = 1
      @line_number = 1
      @last_char = nil

      @current_char = @io.read_char || '\0'
    end

    # Returns the next `Token` in this CSV.
    def next_token : CSVLexer::Token
      case current_char
      when '\0'
        if !empty_column
          @token.kind = Token::Kind::EOF
        end
      when '\r', '\n'
        if !empty_column
          consume_newline
        end
      when @separator
        @token.kind = Token::Kind::Null
        next_char
      when @quote_char
        @token.kind = Token::Kind::String
        @token.string_value = consume_quoted_cell
        cell_end
      else
        @token.kind = Token::Kind::String
        @token.string_value = consume_unquoted_cell
        cell_end
      end

      return @token
    end

    private def consume_quoted_cell
      cell_start

      while true
        case char = next_char
        when '\0'
          raise "Unclosed quote"
        when @quote_char
          case next_char
          when @separator
            break
          when '\r', '\n', '\0'
            break
          when @quote_char
            @buffer << @quote_char
          else
            raise "Expecting comma, newline or end, not #{current_char.inspect}"
          end
        when '\\'
          @buffer << consume_string_escape_sequence
        else
          @buffer << char
        end
      end
      @buffer.to_s
    end

    private def consume_unquoted_cell
      cell_start

      case current_char
      when '0'..'9'
        consume_number
      when '-'
        @buffer << current_char
        case next_char
        when '0'..'9'
          consume_number
        else
          consume_string
        end
      when ' '
        case next_char
        when '0'..'9'
          consume_number
        else
          consume_string
        end
      else
        consume_string
      end
    end

    private def consume_string
      @buffer << current_char

      while true
        case char = next_char
        when '\\'
          @buffer << consume_string_escape_sequence
        when @separator, '\r', '\n', '\0'
          break
        when @quote_char
          raise "Unexpected quote"
        else
          @buffer << char
        end
      end
      @buffer.to_s.strip
    end

    private def consume_string_escape_sequence
      case char = next_char
      when '\\', '"', '/'
        char
      when 'b'
        '\b'
      when 'f'
        '\f'
      when 'n'
        '\n'
      when 'r'
        '\r'
      when 't'
        '\t'
      when 'u'
        hexnum1 = read_hex_number
        if hexnum1 < 0xd800 || hexnum1 >= 0xe000
          hexnum1.unsafe_chr
        elsif hexnum1 < 0xdc00
          if next_char != '\\' || next_char != 'u'
            raise "Unterminated UTF-16 sequence"
          end
          hexnum2 = read_hex_number
          unless 0xdc00 <= hexnum2 <= 0xdfff
            raise "Invalid UTF-16 sequence"
          end
          ((hexnum1 << 10) &+ hexnum2 &- 0x35fdc00).unsafe_chr
        else
          raise "Invalid UTF-16 sequence"
        end
      else
        raise "Unknown escape char: #{char}"
      end
    end

    private def read_hex_number
      hexnum = 0
      4.times do
        char = next_char
        hexnum = (hexnum << 4) | (char.to_i?(16) || raise "Unexpected char in hex number: #{char.inspect}")
      end
      hexnum
    end

    private def consume_number
      case current_char
      when '0'
        append_number_char
        char = next_char
        case char
        when '.'
          consume_float
        when 'e', 'E'
          consume_exponent
        when '0'..'9'
          unexpected_char
        else
          @token.kind = :int
          number_end
        end
      when '1'..'9'
        append_number_char
        char = next_char
        while '0' <= char <= '9'
          append_number_char
          char = next_char
        end

        case char
        when '.'
          consume_float
        when 'e', 'E'
          consume_exponent
        else
          @token.kind = :int
          number_end
        end
      else
        unexpected_char
      end
    end

    private def consume_float
      append_number_char
      char = next_char

      unless '0' <= char <= '9'
        unexpected_char
      end

      while '0' <= char <= '9'
        append_number_char
        char = next_char
      end

      if char.in?('e', 'E')
        consume_exponent
      else
        @token.kind = :float
        number_end
      end
    end

    private def consume_exponent
      append_number_char

      char = next_char
      if char == '+'
        append_number_char
        char = next_char
      elsif char == '-'
        append_number_char
        char = next_char
      end

      if '0' <= char <= '9'
        while '0' <= char <= '9'
          append_number_char
          char = next_char
        end
      else
        unexpected_char
      end

      @token.kind = :float

      number_end
    end

    private def consume_newline
      @token.kind = Token::Kind::Newline
      @column_number = 0
      @line_number += 1

      last_char = current_char
      char = next_char_no_column_increment
      if last_char == '\r'
        if char == '\n'
          next_char_no_column_increment
        end
      end
    end

    private def next_char
      @last_char = current_char
      @column_number += 1
      char = next_char_no_column_increment
      char
    end

    private def next_char_no_column_increment
      @current_char = @io.read_char || '\0'
    end

    private def empty_column
      if @last_char == @separator
        @token.kind = Token::Kind::Null
        @last_char = nil

        return true
      else
        return false
      end
    end

    private def cell_start
      @buffer.clear
    end

    private def append_number_char
      @buffer << current_char
    end

    private def number_string
      @buffer.to_s
    end

    private def number_end
      @token.raw_value = number_string
    end

    private def cell_end
      if current_char == @separator
        next_char
      end
    end

    private def unexpected_char(char = current_char)
      raise "Unexpected char '#{char}'"
    end

    private def raise(msg)
      ::raise CSV::MalformedCSVError.new(msg, @line_number, @column_number)
    end
  end
end
