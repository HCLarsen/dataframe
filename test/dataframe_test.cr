require "minitest/autorun"

require "/../src/dataframe"

class DataframeTest < Minitest::Test
  def test_initializes
    headers = ["Name", "Age", "Address"]
    rows = [
      ["Jim", "41", "Hawkins, Indiana, USA"],
      ["Yuri", "47", "Siberia, USSR"],
      ["Murray", "40", "Sesser, Illinois, USA"]
    ]

    dataframe = Dataframe.new(headers, rows)

    assert_equal headers, dataframe.headers
    assert_equal rows, dataframe.rows
  end

  def test_parses_from_csv
    csv = File.read("./test/files/adults.csv")
    dataframe = Dataframe.from_csv(csv)

    assert_equal ["Name", "Age", "Address"], dataframe.headers
    assert_equal ["Jim","41","Hawkins, Indiana, USA"], dataframe.rows[0]
  end

  def test_parses_from_csv_file
    dataframe = Dataframe.from_csv_file("./test/files/adults.csv")

    assert_equal ["Name", "Age", "Address"], dataframe.headers
    assert_equal ["Jim","41","Hawkins, Indiana, USA"], dataframe.rows[0]
  end

  def test_raises_for_invalid_csv
    error = assert_raises do
      dataframe = Dataframe.from_csv_file("./test/files/uneven.csv")
    end

    assert_equal Dataframe::InvalidDataframeError, error.class
  end

  def test_gets_row_count
    dataframe = Dataframe.from_csv_file("./test/files/adults.csv")
    assert_equal 3, dataframe.row_count
  end

  def test_outputs_to_csv
    filename = "./test/files/adults.csv"
    file = File.read(filename)

    dataframe = Dataframe.from_csv_file(filename)

    assert_equal file, dataframe.to_csv
  end

  def test_gets_columns
    dataframe = Dataframe.from_csv_file("./test/files/adults.csv")

    assert_equal ["Jim", "Yuri", "Murray"], dataframe.columns["Name"]
  end

  def test_inner_joins_dataframes_on_specified_columns
    kids = Dataframe.from_csv_file("./test/files/kids.csv")
    school = Dataframe.from_csv_file("./test/files/school.csv")

    joined = kids.inner_join(school, on: ["Name", "Age"])

    assert_equal 3, joined.rows.size
    assert_equal ["Name", "Age", "Gender", "Grade"], joined.headers
    assert_equal ["Eddie", "20", "Male", "12"], joined.rows[0]
  end

  def test_left_outer_join_dataframes
    kids = Dataframe.from_csv_file("./test/files/kids.csv")
    school = Dataframe.from_csv_file("./test/files/school.csv")

    joined = kids.left_outer_join(school, on: ["Name", "Age"])

    assert_equal 4, joined.rows.size
    assert_equal ["Name", "Age", "Gender", "Grade"], joined.headers
    assert_equal ["El", "15", "Female", ""], joined.rows[2]
  end

  def test_right_outer_joins_dataframes
    kids = Dataframe.from_csv_file("./test/files/kids.csv")
    school = Dataframe.from_csv_file("./test/files/school.csv")

    joined = kids.right_outer_join(school, on: ["Name", "Age"])

    assert_equal 4, joined.rows.size
    assert_equal ["Name", "Age", "Grade", "Gender"], joined.headers
    assert_equal ["Gareth", "17", "11", ""], joined.rows.last
  end

  def test_full_join
    kids = Dataframe.from_csv_file("./test/files/kids.csv")
    school = Dataframe.from_csv_file("./test/files/school.csv")

    joined = kids.full_join(school, on: ["Name", "Age"])

    assert_equal 5, joined.rows.size
    assert_equal ["Name", "Age", "Gender", "Grade"], joined.headers
    assert_equal ["Eddie", "20", "Male", "12"], joined.rows[0]
    assert_equal ["Gareth", "17", "", "11"], joined.rows.last
  end

  def test_modify_column
    dataframe = Dataframe.from_csv_file("./test/files/adults.csv")

    dataframe.modify_column("Address") do |e|
      e.upcase
    end

    dataframe.modify_column("Name") do |e|
      e.downcase
    end

    assert_equal ["jim", "41", "HAWKINS, INDIANA, USA"], dataframe.rows[0]
  end

  def test_remove_duplicates
    headers = ["Name", "Age", "Address"]
    rows = [
      ["Jim", "41", "Hawkins, Indiana, USA"],
      ["Eddie", "20", "Hawkins, Indiana, USA"],
      ["Jim", "41", "Siberia, USSR"],
      ["Yuri", "47", "Siberia, USSR"],
      ["Jim", "41", "Siberia, USSR"]
    ]

    dataframe = Dataframe.new(headers, rows)

    dataframe.remove_duplicates(["Name"])

    expected = [
      ["Jim", "41", "Hawkins, Indiana, USA"],
      ["Eddie", "20", "Hawkins, Indiana, USA"],
      ["Yuri", "47", "Siberia, USSR"]
    ]

    assert_equal expected, dataframe.rows
  end

  def test_remove_duplicates_without_args
    dataframe = Dataframe.from_csv_file("./test/files/duplicates.csv")

    dataframe.remove_duplicates

    expected = [
      ["Jim", "44", "Hawkins, Indiana, USA"],
      ["Joyce", "44", "Hawkins, Indiana, USA"],
      ["Eddie","20","Hawkins, Indiana, USA"],
      ["Jim", "44", "Siberia, USSR"],
      ["Yuri","47","Siberia, USSR"],
      ["Joyce", "44", "Lenora Hills, California, USA"],
    ]

    assert_equal expected, dataframe.rows
  end

  def test_get_duplicates
    dataframe = Dataframe.from_csv_file("./test/files/duplicates.csv")

    duplicates = dataframe.duplicates(["Name", "Age"])

    expected = [
      ["Jim", "44", "Hawkins, Indiana, USA"],
      ["Joyce", "44", "Hawkins, Indiana, USA"],
      ["Jim", "44", "Siberia, USSR"],
      ["Joyce", "44", "Lenora Hills, California, USA"],
      ["Jim", "44", "Hawkins, Indiana, USA"],
    ]

    assert_equal expected, duplicates.rows
  end

  def test_remove_and_return_duplicates
    dataframe = Dataframe.from_csv_file("./test/files/duplicates.csv")

    duplicates = dataframe.duplicates(["Name", "Age"], true)

    expected = [
      ["Eddie","20","Hawkins, Indiana, USA"],
      ["Yuri","47","Siberia, USSR"],
    ]

    expected_duplicates = [
      ["Jim", "44", "Hawkins, Indiana, USA"],
      ["Joyce", "44", "Hawkins, Indiana, USA"],
      ["Jim", "44", "Siberia, USSR"],
      ["Joyce", "44", "Lenora Hills, California, USA"],
      ["Jim", "44", "Hawkins, Indiana, USA"],
    ]

    assert_equal expected, dataframe.rows
    assert_equal expected_duplicates, duplicates.rows
  end

  def test_selects_columns
    dataframe = Dataframe.from_csv_file("./test/files/adults.csv")
    new_headers = ["Name", "Address"]

    ageless = dataframe.select_columns(new_headers)

    assert_equal new_headers, ageless.headers
    assert_equal ["Jim","Hawkins, Indiana, USA"], ageless.rows[0]
  end

  def test_removes_columns
    dataframe = Dataframe.from_csv_file("./test/files/adults.csv")
    new_headers = ["Age", "Address"]

    names = dataframe.remove_columns(new_headers)

    assert_equal new_headers, names.headers
    assert_equal ["Jim"], names.rows[0]
  end

  def test_renames_column
    dataframe = Dataframe.from_csv_file("./test/files/adults.csv")

    dataframe.rename_column("Address", "Location")

    assert_equal ["Name", "Age", "Location"], dataframe.headers
  end

  def test_outputs_table_string
    dataframe = Dataframe.from_csv_file("./test/files/adults.csv")

    expected = "Name    Age  Address              \nJim     41   Hawkins, Indiana, USA\nYuri    47   Siberia, USSR        \nMurray  40   Sesser, Illinois, USA"

    assert_equal expected, dataframe.to_table

    expected = "Name    Age  Address              \nJim     41   Hawkins, Indiana, USA\nYuri    47   Siberia, USSR        "

    assert_equal expected, dataframe.to_table(0..1)
  end
end
