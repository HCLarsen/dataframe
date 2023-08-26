# Dataframe

**Work in Progress** There is still potential for breaking changes.

The Dataframe shard allows programmers to work with and manipulate data in a dataframe (or dataset) object. Information can be easily imported or exported in CSV format, modified by column, filtered by row, or even combined with other Dataframe objects.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     dataframe:
       github: HCLarsen/dataframe
   ```

2. Run `shards install`

## Usage

Dataframes cam be created from column names and data, or imported from CSV or JSON. Data types of columns can be specified during initialization, or they can be automatically determined from the content of the data.

**NOTE** While dataframe columns are always nilable, they're not able to be specified as any other union type. When automatically determining the column type, the type of the first non-nil entry in that column will be used.

```crystal
require "dataframe"

headers = ["Name", "Age", "Address"]
rows = [
  ["Jim", 41, "Hawkins, Indiana, USA"] of Dataframe::Type,
  ["Yuri", 47, "Siberia, USSR"] of Dataframe::Type,
  ["Murray", 40, "Sesser, Illinois, USA"] of Dataframe::Type,
]

dataframe = Dataframe.new(headers, rows)
```

The values in the `Dataframe` can be accessed as an `Array` of arrays called `#data`, an `Array` of `Row` objects, or a `Hash` of `Column` objects.

```crystal
dataframe.shape         #=> {2, 3}
dataframe.data[1][0]    #=> "Yuri"
dataframe.row[1]        #=> Dataframe::Row{ "Name" => "Yuri", "Age" => 47, "Address" => "Siberia, USSR" }
dataframe.column("Age") #=> Dataframe::Column{41, 47, 40}
```

The `Column` class contains many statistical and mathematical methods, such as `#sum`, `#avg`, and `#mode`. While a `Column` in a `Dataframe` can't be modified directly, it can be reassigned using the `#[]=` operator with the column name.

```crystal
age_column = dataframe["Age"].as(Dataframe::Column(Int32))
age_column.map! { |e| e.nil? ? nil : e + 1 }
dataframe["Age"] = age_column

assert_equal dataframe["Age"] #=> Dataframe::Column{42, 48, nil}
```

**NOTE** The reassignment to a column cannot change the type. Attempting to do so will provide a runtime error. If the calculation needed will return a new datatype, it's best to add a new column with values generated by `#map` on the existing column.

## Development

### To Do

- Add fillnil
- Add compact(remove any row with a nil value)
- Add detection of "mergeable" rows.
- Add correlation between two columns.
- Get a sample of rows based on percentage.
- Create a CSV parser/generator.
- Create a JSON parser/generator.

## Contributing

Please ensure that all features are properly tested. Note that instead of Crystal's `spec` library, Dataframe uses [minitest.cr](https://github.com/ysbaddaden/minitest.cr). 

1. Fork it (<https://github.com/HCLarsen/dataframe/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Chris Larsen](https://github.com/HCLarsen) - creator and maintainer
