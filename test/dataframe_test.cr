require "minitest/autorun"

require "/../src/dataframe"

class DataframeTest < Minitest::Test
  @headers = ["Name", "Age", "Address"]
  @data = [
    ["Jim", 41, "Hawkins, Indiana, USA"] of Dataframe::Type,
    ["Yuri", 47, "Siberia, USSR"] of Dataframe::Type,
    ["Murray", 40, "Sesser, Illinois, USA"] of Dataframe::Type,
  ]

  def test_initializes_empty_dataframe
    dataframe = Dataframe.new

    assert_equal [] of String, dataframe.headers
    expected = {0, 0}
    assert_equal expected, dataframe.shape
  end

  def test_initializes_with_headers
    headers = ["Name", "Age", "Address"]
    dataframe = Dataframe.new(headers)

    assert_equal headers, dataframe.headers
    expected = {0, 3}
    assert_equal expected, dataframe.shape
  end

  def test_initializes_with_types
    columns = {"Name" => String, "Age" => Int32, "Address" => String}
    dataframe = Dataframe.new(columns)

    assert_equal columns.keys, dataframe.headers
    expected = {0, 3}
    assert_equal expected, dataframe.shape
  end

  def test_initializes_from_array_of_arrays
    headers = ["Name", "Age", "Address"]
    data = [
      ["Jim", 41, "Hawkins, Indiana, USA"] of Dataframe::Type,
      ["Yuri", 47, "Siberia, USSR"] of Dataframe::Type,
      ["Murray", 40, "Sesser, Illinois, USA"] of Dataframe::Type,
    ]

    dataframe = Dataframe.new(headers, data)

    assert_equal headers, dataframe.headers
    assert_equal data, dataframe.data
  end

  def test_raises_for_uneven_rows
    headers = ["Name", "Age", "Address"]
    data = [
      ["Jim", 41, "Hawkins, Indiana, USA"] of Dataframe::Type,
      ["Yuri", 47, "Siberia, USSR"] of Dataframe::Type,
      ["Murray", 40] of Dataframe::Type,
    ]

    error = assert_raises do
      dataframe = Dataframe.new(headers, data)
    end

    assert_equal Dataframe::InvalidDataframeError, error.class
  end

  def test_adds_data_rows
    first_row = ["Jim", 41, "Hawkins, Indiana, USA"] of Dataframe::Type
    second_row = ["Yuri", 47, "Siberia, USSR"] of Dataframe::Type
    columns = {"Name" => String, "Age" => Int32, "Address" => String}
    dataframe = Dataframe.new(columns)

    dataframe.add_row(first_row)
    assert_equal first_row, dataframe.data[0]
    expected = {1, 3}
    assert_equal expected, dataframe.shape

    dataframe << second_row
    assert_equal second_row, dataframe.data[1]
    expected = {2, 3}
    assert_equal expected, dataframe.shape
  end

  def test_raises_for_wrong_data_row_size
    columns = {"Name" => String, "Age" => Int32, "Address" => String}
    dataframe = Dataframe.new(columns)

    row = ["Jim", 41] of Dataframe::Type

    error = assert_raises do
      dataframe.add_row(row)
    end

    assert_equal Dataframe::InvalidRowError, error.class
    assert_equal "Row has different size than Dataframe", error.message
  end

  def test_raises_for_invalid_data_row_type
    columns = {"Name" => String, "Age" => Int32, "Address" => String}
    dataframe = Dataframe.new(columns)

    new_row = ["Joyce", "44", "Hawkins, Indiana, USA"] of Dataframe::Type

    error = assert_raises do
      dataframe.add_row(new_row)
    end

    assert_equal "Invalid type for column \"Age\". Expected (Int32 | Nil), but got String", error.message
  end

  def test_adds_rows
    columns = {"Name" => String, "Age" => Int32, "Address" => String}
    first_row = Dataframe::Row.new(["Jim", 41, "Hawkins, Indiana, USA"] of Dataframe::Type, columns.keys)
    second_row = Dataframe::Row.new(["Yuri", 47, "Siberia, USSR"] of Dataframe::Type, columns.keys)

    dataframe = Dataframe.new(columns)

    dataframe.add_row(first_row)
    assert_equal first_row, dataframe.rows[0]

    dataframe << second_row
    assert_equal second_row, dataframe.rows[1]
  end

  def test_adds_incomplete_rows
    headers = ["Name", "Age", "Address"]
    data = [
      ["Yuri", 47, "Siberia, USSR"] of Dataframe::Type,
      ["Murray", 40, "Sesser, Illinois, USA"] of Dataframe::Type,
    ]

    dataframe = Dataframe.new(headers, data)

    row = Dataframe::Row{"Name" => "Jim", "Address" => "Hawkins, Indiana, USA"}
    dataframe << row

    assert_equal ["Jim", nil, "Hawkins, Indiana, USA"], dataframe.data[2]
  end

  def test_raises_for_invalid_rows
    headers = ["Name", "Age", "Address"]
    data = [
      ["Yuri", 47, "Siberia, USSR"] of Dataframe::Type,
      ["Murray", 40, "Sesser, Illinois, USA"] of Dataframe::Type,
    ]

    dataframe = Dataframe.new(headers, data)

    row = Dataframe::Row{"Name" => "Jim", "Location" => "Hawkins, Indiana, USA"}

    error = assert_raises do
      dataframe << row
    end

    assert_equal "Column \"Location\" does not exist in Dataframe", error.message

    row = Dataframe::Row{"Name" => "Jim", "Age" => "41", "Address" => "Hawkins, Indiana, USA"}

    error = assert_raises do
      dataframe.add_row(row)
    end

    assert_equal "Invalid type for column \"Age\". Expected (Int32 | Nil), but got String", error.message
  end

  def test_iterates
    dataframe = Dataframe.new(@headers, @data)

    kids_names = ""

    dataframe.each do |row|
      kids_names += "#{row[0]} "
    end

    assert_equal "Jim Yuri Murray ", kids_names
  end

  def test_iterates_over_rows
    dataframe = Dataframe.new(@headers, @data)

    kids_names = ""

    dataframe.each_row do |row|
      kids_names += "#{row["Name"]} "
    end

    assert_equal "Jim Yuri Murray ", kids_names
  end

  # def test_filters_with_select
  #   dataframe = Dataframe.from_csv_file("./test/files/school.csv")

  #   hawkins = dataframe.select_data { |e| e[2] == "9" }

  #   assert_equal ["Mike","15","9"], hawkins.data[0]
  #   assert_equal ["Dustin","15","9"], hawkins.data[1]
  # end

  # def test_filters_in_place_with_select
  #   dataframe = Dataframe.from_csv_file("./test/files/school.csv")

  #   dataframe.select_data! { |e| e[2] == "9" }

  #   assert_equal ["Mike","15","9"], dataframe.data[0]
  #   assert_equal ["Dustin","15","9"], dataframe.data[1]
  # end

  def test_gets_columns
    dataframe = Dataframe.new(@headers, @data)

    columns = dataframe.columns

    assert_equal ["Jim", "Yuri", "Murray"], columns["Name"].to_a
  end

  def test_adds_empty_column
    dataframe = Dataframe.new(@headers, @data)

    dataframe.add_column("Married?", Bool)

    assert_equal ["Name", "Age", "Address", "Married?"], dataframe.headers
    assert_equal Dataframe::Column(Bool), dataframe.columns["Married?"].class
    assert_equal [nil, nil, nil], dataframe.columns["Married?"].to_a
    assert_equal ["Jim", 41, "Hawkins, Indiana, USA", nil], dataframe.data[0]
  end

  def test_adds_column_with_values
    dataframe = Dataframe.new(@headers, @data)

    dataframe.add_column("Married?", [false, false, false])

    assert_equal ["Name", "Age", "Address", "Married?"], dataframe.headers
    assert_equal ["Jim", 41, "Hawkins, Indiana, USA", false], dataframe.data[0]
  end

  # This feature will generate a new column by iterating over existing row objects, and generating the value for the new column based on the return value of the block.
  # def test_add_column_with_block
  # end

  def test_raises_for_invalid_column
    dataframe = Dataframe.new(@headers, @data)

    error = assert_raises do
      dataframe.add_column("Married?", [false, false])
    end

    assert_equal "New column must be same size as other columns: 3", error.message
  end

  def test_renames_column
    dataframe = Dataframe.new(@headers, @data)

    dataframe.rename_column("Name", "Full Name")

    assert_equal ["Full Name", "Age", "Address"], dataframe.headers
  end

  def test_filters_columns
    dataframe = Dataframe.new(@headers, @data)
    new_headers = ["Name", "Address"]

    ageless = dataframe.select_columns(new_headers)

    assert_equal new_headers, ageless.headers
    assert_equal ["Jim", "Hawkins, Indiana, USA"], ageless.data[0]
  end

  def test_filters_columns_in_place
    dataframe = Dataframe.new(@headers, @data)
    new_headers = ["Name", "Address"]

    dataframe.select_columns!(new_headers)

    assert_equal new_headers, dataframe.headers
    assert_equal ["Jim", "Hawkins, Indiana, USA"], dataframe.data[0]
  end

  def test_removes_columns
    dataframe = Dataframe.new(@headers, @data)
    removing_headers = ["Age", "Address"]

    names = dataframe.reject_columns(removing_headers)

    assert_equal ["Name"], names.headers
    assert_equal ["Jim"], names.data[0]
  end

  def test_removes_columns_in_place
    dataframe = Dataframe.new(@headers, @data)
    removing_headers = ["Age", "Address"]

    dataframe.reject_columns!(removing_headers)

    assert_equal ["Name"], dataframe.headers
    assert_equal ["Jim"], dataframe.data[0]
  end

  def test_rearranges_columns
    new_headers = ["Name", "Address", "Age"]
    dataframe = Dataframe.new(@headers, @data)

    new_dataframe = dataframe.order_columns(["Name", "Address", "Age"])

    assert_equal new_headers, new_dataframe.headers
    assert_equal ["Jim", "Hawkins, Indiana, USA", 41], new_dataframe.data.first?

    # Confirm original is unchanged
    assert_equal @headers, dataframe.headers
  end

  # def test_modify_column
  #   dataframe = Dataframe.new(@headers, @data)

  #   dataframe.modify_column("Address") do |e|
  #     e.upcase
  #   end

  #   dataframe.modify_column("Name") do |e|
  #     e.downcase
  #   end

  #   assert_equal ["jim", "41", "HAWKINS, INDIANA, USA"], dataframe.data[0]
  # end

  # def test_parses_from_csv
  #   csv = File.read("./test/files/adults.csv")
  #   dataframe = Dataframe.from_csv(csv)

  #   assert_equal ["Name", "Age", "Address"], dataframe.headers
  #   assert_equal ["Jim","41","Hawkins, Indiana, USA"], dataframe.data[0]
  # end

  # def test_parses_from_csv_file
  #   dataframe = Dataframe.from_csv_file("./test/files/adults.csv")

  #   assert_equal ["Name", "Age", "Address"], dataframe.headers
  #   assert_equal ["Jim","41","Hawkins, Indiana, USA"], dataframe.data[0]
  # end

  # def test_raises_for_invalid_csv
  #   error = assert_raises do
  #     dataframe = Dataframe.from_csv_file("./test/files/uneven.csv")
  #   end

  #   assert_equal Dataframe::InvalidDataframeError, error.class
  # end

  # def test_outputs_to_csv
  #   filename = "./test/files/adults.csv"
  #   file = File.read(filename)

  #   dataframe = Dataframe.from_csv_file(filename)

  #   assert_equal file, dataframe.to_csv
  # end

  # def test_inner_joins_dataframes_on_specified_columns
  #   kids = Dataframe.from_csv_file("./test/files/kids.csv")
  #   school = Dataframe.from_csv_file("./test/files/school.csv")

  #   joined = kids.inner_join(school, on: ["Name", "Age"])

  #   assert_equal 3, joined.data.size
  #   assert_equal ["Name", "Age", "Gender", "Grade"], joined.headers
  #   assert_equal ["Eddie", "20", "Male", "12"], joined.data[0]
  # end

  # def test_left_outer_join_dataframes
  #   kids = Dataframe.from_csv_file("./test/files/kids.csv")
  #   school = Dataframe.from_csv_file("./test/files/school.csv")

  #   joined = kids.left_outer_join(school, on: ["Name", "Age"])

  #   assert_equal 4, joined.data.size
  #   assert_equal ["Name", "Age", "Gender", "Grade"], joined.headers
  #   assert_equal ["El", "15", "Female", ""], joined.data[2]
  # end

  # def test_right_outer_joins_dataframes
  #   kids = Dataframe.from_csv_file("./test/files/kids.csv")
  #   school = Dataframe.from_csv_file("./test/files/school.csv")

  #   joined = kids.right_outer_join(school, on: ["Name", "Age"])

  #   assert_equal 4, joined.data.size
  #   assert_equal ["Name", "Age", "Grade", "Gender"], joined.headers
  #   assert_equal ["Gareth", "17", "11", ""], joined.data.last
  # end

  # def test_full_join
  #   kids = Dataframe.from_csv_file("./test/files/kids.csv")
  #   school = Dataframe.from_csv_file("./test/files/school.csv")

  #   joined = kids.full_join(school, on: ["Name", "Age"])

  #   assert_equal 5, joined.data.size
  #   assert_equal ["Name", "Age", "Gender", "Grade"], joined.headers
  #   assert_equal ["Eddie", "20", "Male", "12"], joined.data[0]
  #   assert_equal ["Gareth", "17", "", "11"], joined.data.last
  # end

  # def test_remove_duplicates
  #   headers = ["Name", "Age", "Address"]
  #   data = [
  #     ["Jim", "41", "Hawkins, Indiana, USA"],
  #     ["Eddie", "20", "Hawkins, Indiana, USA"],
  #     ["Jim", "41", "Siberia, USSR"],
  #     ["Yuri", "47", "Siberia, USSR"],
  #     ["Jim", "41", "Siberia, USSR"]
  #   ]

  #   dataframe = Dataframe.new(headers, data)

  #   dataframe.remove_duplicates(["Name"])

  #   expected = [
  #     ["Jim", "41", "Hawkins, Indiana, USA"],
  #     ["Eddie", "20", "Hawkins, Indiana, USA"],
  #     ["Yuri", "47", "Siberia, USSR"]
  #   ]

  #   assert_equal expected, dataframe.data
  # end

  # def test_remove_duplicates_without_args
  #   dataframe = Dataframe.from_csv_file("./test/files/duplicates.csv")

  #   dataframe.remove_duplicates

  #   expected = [
  #     ["Jim", "44", "Hawkins, Indiana, USA"],
  #     ["Joyce", "44", "Hawkins, Indiana, USA"],
  #     ["Eddie","20","Hawkins, Indiana, USA"],
  #     ["Jim", "44", "Siberia, USSR"],
  #     ["Yuri","47","Siberia, USSR"],
  #     ["Joyce", "44", "Lenora Hills, California, USA"],
  #   ]

  #   assert_equal expected, dataframe.data
  # end

  # def test_get_duplicates
  #   dataframe = Dataframe.from_csv_file("./test/files/duplicates.csv")

  #   duplicates = dataframe.duplicates(["Name", "Age"])

  #   expected = [
  #     ["Jim", "44", "Hawkins, Indiana, USA"],
  #     ["Joyce", "44", "Hawkins, Indiana, USA"],
  #     ["Jim", "44", "Siberia, USSR"],
  #     ["Joyce", "44", "Lenora Hills, California, USA"],
  #     ["Jim", "44", "Hawkins, Indiana, USA"],
  #   ]

  #   assert_equal expected, duplicates.data
  # end

  # def test_remove_and_return_duplicates
  #   dataframe = Dataframe.from_csv_file("./test/files/duplicates.csv")

  #   duplicates = dataframe.duplicates(["Name", "Age"], true)

  #   expected = [
  #     ["Eddie","20","Hawkins, Indiana, USA"],
  #     ["Yuri","47","Siberia, USSR"],
  #   ]

  #   expected_duplicates = [
  #     ["Jim", "44", "Hawkins, Indiana, USA"],
  #     ["Joyce", "44", "Hawkins, Indiana, USA"],
  #     ["Jim", "44", "Siberia, USSR"],
  #     ["Joyce", "44", "Lenora Hills, California, USA"],
  #     ["Jim", "44", "Hawkins, Indiana, USA"],
  #   ]

  #   assert_equal expected, dataframe.data
  #   assert_equal expected_duplicates, duplicates.data
  # end

  # def test_outputs_table_string
  #   dataframe = Dataframe.from_csv_file("./test/files/adults.csv")

  #   expected = "Name    Age  Address              \nJim     41   Hawkins, Indiana, USA\nYuri    47   Siberia, USSR        \nMurray  40   Sesser, Illinois, USA"

  #   assert_equal expected, dataframe.to_table

  #   expected = "Name    Age  Address              \nJim     41   Hawkins, Indiana, USA\nYuri    47   Siberia, USSR        "

  #   assert_equal expected, dataframe.to_table(0..1)
  # end
end
