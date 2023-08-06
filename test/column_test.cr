require "minitest/autorun"

require "/../src/column"

class ColumnTest < Minitest::Test
  def test_initializes_as_empty
    column = Dataframe::Column(Int32).new

    assert_equal 0, column.size
  end

  def test_initializes_from_array
    array = [44, 44, 47, 40] of Dataframe::Type
    column = Dataframe::Column(Int32).new(array)

    assert_equal 4, column.size
    assert_equal array, column.to_a
    assert_equal Array(Int32 | Nil), column.to_a.class
  end

  def test_raises_for_value_type_mismatch
    array = [44, 44, 47, 40] of Dataframe::Type

    assert_raises do
      Dataframe::Column(String).new(array)
    end
  end

  def test_raises_for_invalid_type
    expected = "Expected type to be (Bool | Float64 | Int32 | String), not Char"

    error = assert_raises do
      Dataframe::Column(Char).new
    end

    assert_equal expected, error.message
  end

  def test_equality
    array = [44, 44, 47, 40] of Dataframe::Type
    int_column1 = Dataframe::Column(Int32).new(array)
    int_column2 = Dataframe::Column(Int32).new(array)
    int_column3 = Dataframe::Column(Int32).new([nil, nil, nil])
    bool_column1 = Dataframe::Column(Bool).new([nil, nil, nil])

    assert_equal int_column1, int_column2
    refute_equal int_column3, bool_column1
  end

  def test_outputs_to_string
    array = [44, 44, 47, 40] of Dataframe::Type
    column = Dataframe::Column(Int32).new(array)

    assert_equal %(Dataframe::Column{44, 44, 47, 40}), column.to_s
  end

  def test_performs_math_functions
    array = [44, 44, 47, 40] of Dataframe::Type
    int_column = Dataframe::Column(Int32).new(array)

    assert_equal 175, int_column.sum
    assert_equal 43.75, int_column.avg
    assert_equal 47, int_column.max
    assert_equal 40, int_column.min

    array = [44.0, 44.0, 47.0, 40.0] of Dataframe::Type
    float_column = Dataframe::Column(Float64).new(array)

    assert_equal 175.0, float_column.sum
    assert_equal 43.75, float_column.avg
  end

  def test_raises_on_math_on_nonnumeric_column
    array = ["Joyce", "Jim", "Yuri", "Murray"] of Dataframe::Type
    string_column = Dataframe::Column(String).new(array)

    error = assert_raises do
      string_column.sum
    end

    assert_equal Dataframe::NonNumericTypeError, error.class
    assert_equal "Calling numeric method on String Column", error.message

    array = [false, true, true, true] of Dataframe::Type
    bool_column = Dataframe::Column(Bool).new(array)

    error = assert_raises do
      bool_column.avg
    end

    assert_equal Dataframe::NonNumericTypeError, error.class
    assert_equal "Calling numeric method on Bool Column", error.message
  end

  def test_returns_modes
    array = [false, true, true, true] of Dataframe::Type
    bool_column = Dataframe::Column(Bool).new(array)

    assert_equal [true], bool_column.mode

    array = [44, 44, 47, 40, nil, nil] of Dataframe::Type
    int_column = Dataframe::Column(Int32).new(array)

    assert_equal [44], int_column.mode

    string_array = ["Joyce", "Jim", "Yuri", "Murray"] of Dataframe::Type
    string_column = Dataframe::Column(String).new(string_array)

    assert_equal [] of String, string_column.mode
  end

  def test_maps_column
    array = [44, 44, 47, 40] of Dataframe::Type
    column = Dataframe::Column(Int32).new(array)

    new_column = column.map do |e|
      if e.nil?
        e
      else
        e + 1
      end
    end
  end
end
