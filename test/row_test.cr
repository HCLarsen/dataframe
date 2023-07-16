require "minitest/autorun"

require "/../src/row"

class RowTest < Minitest::Test
  def test_initializes_empty_row_object
    row = Dataframe::Row.new

    assert_equal Array(String).new, row.headers
    assert_equal Array(Dataframe::Type).new, row.to_a
    assert_equal Hash(String, Dataframe::Type).new, row.to_h
  end

  def test_initializes_from_columns_and_data
    headers = ["Name", "Age", "Address"]
    array = ["Jim", 44, "Hawkins, Indiana, USA"] of Dataframe::Type

    row = Dataframe::Row.new(array, headers)

    assert_equal array, row.to_a
    assert_equal headers, row.headers
    assert_equal Hash.zip(headers, array), row.to_h
  end

  def test_initializes_hash_like
    row = Dataframe::Row{"Name" => "Jim", "Age" => 44}

    assert_equal "Jim", row["Name"]
    assert_equal 44, row["Age"]
  end

  def test_gets_and_assigns_value_with_brackets
    headers = ["Name", "Age", "Address"]
    array = ["Jim", 41, "Hawkins, Indiana, USA"] of Dataframe::Type

    row = Dataframe::Row.new(array, headers)

    assert_equal 41, row["Age"]

    row["Age"] = 44
    assert_equal 44, row["Age"]
  end

  def test_outputs_to_string
    row = Dataframe::Row{"Name" => "Jim", "Age" => 44}

    assert_equal %(Dataframe::Row{"Name" => "Jim", "Age" => 44}), row.to_s
  end
end
