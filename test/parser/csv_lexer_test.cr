require "minitest/autorun"

require "/../src/parser/csv_lexer"

class CSVParserTest < Minitest::Test
  private def assert_lexes_string(string : String, expected_value : String)
    lexer = Dataframe::CSVLexer.new string
    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::String, token.kind
    assert_equal expected_value, token.string_value

    lexer = Dataframe::CSVLexer.new IO::Memory.new(string)
    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::String, token.kind
    assert_equal expected_value, token.string_value
  end

  private def assert_lexes_int(string : String, expected_value : Int)
    lexer = Dataframe::CSVLexer.new string
    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::Int, token.kind
    assert_equal expected_value, token.int_value

    lexer = Dataframe::CSVLexer.new IO::Memory.new(string)
    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::Int, token.kind
    assert_equal expected_value, token.int_value
  end

  private def assert_lexes_float(string : String, expected_value : Float)
    lexer = Dataframe::CSVLexer.new string
    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::Float, token.kind
    assert_equal expected_value, token.float_value

    lexer = Dataframe::CSVLexer.new IO::Memory.new(string)
    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::Float, token.kind
    assert_equal expected_value, token.float_value
  end

  private def assert_string_cell(lexer : Dataframe::CSVLexer, expected_value : String)
    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::String, token.kind
    assert_equal expected_value, token.string_value
  end

  private def assert_nil_cell(lexer : Dataframe::CSVLexer)
    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::Null, token.kind
  end

  def test_lexes_value_types
    assert_lexes_string "hello", "hello"
    assert_lexes_string "true", "true"
    assert_lexes_string "false", "false"
    assert_lexes_string "-----", "-----"
    assert_lexes_string "hello\\\"world", "hello\"world"
    assert_lexes_string "hello\\\\world", "hello\\world"
    assert_lexes_string "hello\\/world", "hello/world"
    assert_lexes_string "hello\\bworld", "hello\bworld"
    assert_lexes_string "hello\\fworld", "hello\fworld"
    assert_lexes_string "hello\\nworld", "hello\nworld"
    assert_lexes_string "hello\\rworld", "hello\rworld"
    assert_lexes_string "hello\\tworld", "hello\tworld"
    assert_lexes_string "\"hello\"", "hello"
    assert_lexes_string "\"hello\\\"world\"", "hello\"world"
    assert_lexes_string "\"hello\\\\world\"", "hello\\world"
    assert_lexes_string "\"hello\\/world\"", "hello/world"
    assert_lexes_string "\"hello\\bworld\"", "hello\bworld"
    assert_lexes_string "\"hello\\fworld\"", "hello\fworld"
    assert_lexes_string "\"hello\\nworld\"", "hello\nworld"
    assert_lexes_string "\"hello\\rworld\"", "hello\rworld"
    assert_lexes_string "\"hello\\tworld\"", "hello\tworld"
    assert_lexes_string "\"\\u201chello world\\u201d\"", "“hello world”"
    assert_lexes_string "\"\\uD800\\uDC00\"", 0x10000.unsafe_chr.to_s
    assert_lexes_string "\"\\uD840\\uDC00\"", 0x20000.unsafe_chr.to_s
    assert_lexes_string "\"\\uDBFF\\uDFFF\"", 0x10ffff.unsafe_chr.to_s
    assert_lexes_string "\"\\uD834\\uDD1E\"", "𝄞"

    assert_lexes_int "0", 0
    assert_lexes_int "1", 1
    assert_lexes_int "1234", 1234
    assert_lexes_int "-1", -1
    assert_lexes_float "0.123", 0.123
    assert_lexes_float "1234.567", 1234.567
    assert_lexes_float "0e1", 0.0
    assert_lexes_float "0E1", 0.0
    assert_lexes_float "0.1e1", 0.1e1
    assert_lexes_float "0e+12", 0.0
    assert_lexes_float "0e-12", 0.0
    assert_lexes_float "1e2", 1e2
    assert_lexes_float "1E2", 1e2
    assert_lexes_float "1e+12", 1e12
    assert_lexes_float "1.2e-3", 1.2e-3
    assert_lexes_float "9.91343313498688", 9.91343313498688
    assert_lexes_float "-1.23", -1.23
    assert_lexes_float "-1.23e4", -1.23e4
    assert_lexes_float "-1.23e4", -1.23e4
    assert_lexes_float "1000000000000000000.0", 1000000000000000000.0
    assert_lexes_float "6000000000000000000.0", 6000000000000000000.0
    assert_lexes_float "9000000000000000000.0", 9000000000000000000.0
    assert_lexes_float "9876543212345678987654321.0", 9876543212345678987654321.0
    assert_lexes_float "9876543212345678987654321e20", 9876543212345678987654321e20
    assert_lexes_float "10.100000000000000000000", 10.1
  end

  def test_lexes_three_columns
    lexer = Dataframe::CSVLexer.new("one,2,3.0")

    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::String, token.kind
    assert_equal "one", token.string_value

    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::Int, token.kind
    assert_equal 2, token.int_value

    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::Float, token.kind
    assert_equal 3.0, token.float_value

    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_three_columns_with_whitespace
    lexer = Dataframe::CSVLexer.new("one, 2, 3.0")

    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::String, token.kind
    assert_equal "one", token.string_value

    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::Int, token.kind
    assert_equal 2, token.int_value

    token = lexer.next_token
    assert_equal Dataframe::CSVLexer::Token::Kind::Float, token.kind
    assert_equal 3.0, token.float_value

    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_two_columns_with_two_rows
    lexer = Dataframe::CSVLexer.new("hello,world\nfoo,bar")

    assert_string_cell lexer, "hello"
    assert_string_cell lexer, "world"
    assert_equal Dataframe::CSVLexer::Token::Kind::Newline, lexer.next_token.kind

    assert_string_cell lexer, "foo"
    assert_string_cell lexer, "bar"
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_carriage_return_and_newline
    lexer = Dataframe::CSVLexer.new("hello,world\r\nfoo,bar")

    assert_string_cell lexer, "hello"
    assert_string_cell lexer, "world"
    assert_equal Dataframe::CSVLexer::Token::Kind::Newline, lexer.next_token.kind

    assert_string_cell lexer, "foo"
    assert_string_cell lexer, "bar"
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_two_empty_columns
    lexer = Dataframe::CSVLexer.new(",")
    assert_nil_cell lexer
    assert_nil_cell lexer
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_last_empty_column
    lexer = Dataframe::CSVLexer.new("foo,")
    assert_string_cell lexer, "foo"
    assert_nil_cell lexer
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_with_empty_columns
    lexer = Dataframe::CSVLexer.new("foo,,bar")
    assert_string_cell lexer, "foo"
    assert_nil_cell lexer
    assert_string_cell lexer, "bar"
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_with_empty_column_before_newline
    lexer = Dataframe::CSVLexer.new("foo,\nbar")
    assert_string_cell lexer, "foo"
    assert_nil_cell lexer
    assert_equal Dataframe::CSVLexer::Token::Kind::Newline, lexer.next_token.kind
    assert_string_cell lexer, "bar"
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_with_whitespace
    lexer = Dataframe::CSVLexer.new(%("  foo  ","  bar  "))
    assert_string_cell lexer, "  foo  "
    assert_string_cell lexer, "  bar  "
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_two_with_quotes
    lexer = Dataframe::CSVLexer.new(%("hello","world"))
    assert_string_cell lexer, "hello"
    assert_string_cell lexer, "world"
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_two_with_inner_quotes
    lexer = Dataframe::CSVLexer.new(%("hel""lo","wor""ld"))
    assert_string_cell lexer, %(hel"lo)
    assert_string_cell lexer, %(wor"ld)
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_with_comma_inside_quote
    lexer = Dataframe::CSVLexer.new(%("foo,bar"))
    assert_string_cell lexer, "foo,bar"
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_with_newline_inside_quote
    lexer = Dataframe::CSVLexer.new(%("foo\nbar"))
    assert_string_cell lexer, "foo\nbar"
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_newline_followed_by_eof
    lexer = Dataframe::CSVLexer.new("hello,world\n")
    assert_string_cell lexer, "hello"
    assert_string_cell lexer, "world"
    assert_equal Dataframe::CSVLexer::Token::Kind::Newline, lexer.next_token.kind
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_with_a_given_separator
    lexer = Dataframe::CSVLexer.new("hello;world\n", separator: ';')
    assert_string_cell lexer, "hello"
    assert_string_cell lexer, "world"
    assert_equal Dataframe::CSVLexer::Token::Kind::Newline, lexer.next_token.kind
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_lexes_with_a_given_quote_char
    lexer = Dataframe::CSVLexer.new("'hello,world'\n", quote_char: '\'')
    assert_string_cell lexer, "hello,world"
    assert_equal Dataframe::CSVLexer::Token::Kind::Newline, lexer.next_token.kind
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_raises_if_single_quote_in_the_middle
    error = assert_raises do
      lexer = Dataframe::CSVLexer.new(%(hel"lo))
      lexer.next_token
    end

    assert_equal CSV::MalformedCSVError, error.class
    assert_equal "Unexpected quote at line 1, column 4", error.message
  end

  def test_raises_if_command_newline_or_end_not_after_quote
    error = assert_raises do
      lexer = Dataframe::CSVLexer.new(%("hel"a))
      lexer.next_token
    end

    assert_equal CSV::MalformedCSVError, error.class
    assert_equal "Expecting comma, newline or end, not 'a' at line 1, column 6", error.message
  end

  def test_raises_on_unclose_quote
    error = assert_raises do
      lexer = Dataframe::CSVLexer.new(%("foo))
      lexer.next_token
    end

    assert_equal CSV::MalformedCSVError, error.class
    assert_equal "Unclosed quote at line 1, column 5", error.message
  end

  def test_doesnt_consume_char_after_slash_n
    io = IO::Memory.new("a\n")
    lexer = Dataframe::CSVLexer.new(io)

    assert_string_cell lexer, "a"
    assert_equal Dataframe::CSVLexer::Token::Kind::Newline, lexer.next_token.kind
    assert_equal 2, io.pos
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end

  def test_doesnt_consume_char_after_slash_r
    io = IO::Memory.new("a\r\nx")
    lexer = Dataframe::CSVLexer.new(io)

    assert_string_cell lexer, "a"
    assert_equal 2, io.pos
    assert_equal Dataframe::CSVLexer::Token::Kind::Newline, lexer.next_token.kind
    assert_string_cell lexer, "x"
    assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  end
end
