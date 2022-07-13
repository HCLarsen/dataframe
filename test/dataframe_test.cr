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
    dataframe = Dataframe.from_csv_file("./test/files/adults.csv")

    assert_equal ["Name", "Age", "Address"], dataframe.headers
    assert_equal ["Jim","41","Hawkins, Indiana, USA"], dataframe.rows[0]
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
end
