# **NOTE** The CSV Spec(RFC 4180) does state that leading and trailing whitespaces
# should not be ignored, some implementations of CSV do still use them. For this reason,
# `Dataframe` will ignore leading and trailing whitespace in unquoted cells.
class Dataframe::CSVParser
end
