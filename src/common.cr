class Dataframe
  # nodoc
  TYPES = [String, Int32, Float64, Bool]

  alias Type = String | Int32 | Float64 | Bool | Nil
  alias ColumnType = String.class | Int32.class | Float64.class | Bool.class

  # Raised when an error is encountered during parsing.
  class InvalidDataframeError < Exception
    def initialize(message = "Rows are of uneven length")
      super(message)
    end
  end

  class InvalidTypeError < Exception
    def initialize(class_name : Class)
      types = TYPES.map { |e| e.to_s }.sort.join(" | ")

      message = "Expected type to be (#{types}), not #{class_name}"

      super(message)
    end

    def initialize(message : String)
      super(message)
    end
  end

  class InvalidRowError < Exception
    def initialize(message = "Row is an invalid match for Dataframe")
      super(message)
    end
  end

  class NonNumericTypeError < Exception
    def initialize(class_name : Class)
      message = "Calling numeric method on #{class_name} Column"

      super(message)
    end
  end
end
