require "minitest/autorun"

require "/../src/parser/csv_parser"

class CSVParserTest < Minitest::Test
  def test_parses_empty_string
    dataframe = Dataframe.from_csv("")

    assert_equal [] of String, dataframe.headers
    expected = {0, 0}
    assert_equal expected, dataframe.shape

    dataframe = Dataframe.from_csv("", headers: false)

    assert_equal [] of String, dataframe.headers
    expected = {0, 0}
    assert_equal expected, dataframe.shape
  end

  def test_parses_csv_with_headers
    csv = File.read("./test/files/adults.csv")
    dataframe = Dataframe.from_csv(csv)

    assert_equal ["Name", "Age", "Address"], dataframe.headers
    assert_equal 4, dataframe.row_count
    assert_equal ["Joyce", 44, "Lenora Hill, California, USA"], dataframe.data[0]
    assert_equal ["Murray", nil, "Sesser, Illinois, USA"], dataframe.data[3]
  end

  def test_parses_csv_without_headers
    csv = "a,b,c,d,e,f,g\nh,i,j,k,l,m,n"
    dataframe = Dataframe.from_csv(csv, headers: false)

    assert_equal ["c0", "c1", "c2", "c3", "c4", "c5", "c6"], dataframe.headers
    assert_equal ["a", "b", "c", "d", "e", "f", "g"], dataframe.data[0]
    assert_equal ["h", "i", "j", "k", "l", "m", "n"], dataframe.data[1]
  end

  def test_parses_from_csv_file
    file = File.open("./test/files/adults.csv")
    dataframe = Dataframe.from_csv(file)

    assert_equal ["Name", "Age", "Address"], dataframe.headers
    assert_equal ["Joyce", 44, "Lenora Hill, California, USA"], dataframe.data[0]
    assert_equal ["Murray", nil, "Sesser, Illinois, USA"], dataframe.data[3]
  end

  def test_raises_for_uneven_rows
    error = assert_raises do
      dataframe = Dataframe.from_csv(File.open("./test/files/uneven.csv"))
    end

    assert_equal Dataframe::InvalidDataframeError, error.class
    assert_equal "Row 3 has unequal size to other rows", error.message
  end

  def test_raises_for_mismatching_types
    error = assert_raises do
      dataframe = Dataframe.from_csv(File.open("./test/files/bad_type.csv"))
    end

    assert_equal Dataframe::InvalidTypeError, error.class
    assert_equal "Unexpected String in Age column, row 2. Column type is Int", error.message
  end
end
