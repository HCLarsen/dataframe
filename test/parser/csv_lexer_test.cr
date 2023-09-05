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

  # def test_lexes_two_columns
  #   lexer = Dataframe::CSVLexer.new("one,2,3.0,false,")

  #   token = lexer.next_token
  #   assert_equal Dataframe::CSVLexer::Token::Kind::String, token.kind
  #   assert_equal "one", token.string_value

  #   token = lexer.next_token
  #   assert_equal Dataframe::CSVLexer::Token::Kind::Int, token.kind
  #   assert_equal 2, token.int_value

  #   token = lexer.next_token
  #   assert_equal Dataframe::CSVLexer::Token::Kind::Float, token.kind
  #   assert_equal 3.0, token.float_value

  #   token = lexer.next_token
  #   assert_equal Dataframe::CSVLexer::Token::Kind::False, token.kind

  #   assert_equal Dataframe::CSVLexer::Token::Kind::EOF, lexer.next_token.kind
  # end
end
