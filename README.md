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

```crystal
require "dataframe"
```

TODO: Write usage instructions here

## Development

### To Do

1. Add filtering by column.
2. Add filtering of rows.
3. Add detection of "mergeable" rows.

## Contributing

1. Fork it (<https://github.com/HCLarsen/dataframe/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Chris Larsen](https://github.com/HCLarsen) - creator and maintainer
