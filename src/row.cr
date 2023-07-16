class Dataframe
  class Row
    @data : Hash(String, Type)

    # Creates an empty `Row`.
    def initialize
      @data = Hash(String, Type).new
    end

    # Creates a new `Row` with the specified *headers* and *values*.
    def initialize(values : Array(Type), headers : Array(String))
      @data = Hash.zip(headers, values)
    end

    def ==(other : Row) : Bool
      to_h == other.to_h
    end

    # Returns the value for the key given by *key*.
    def [](key : String) : Type
      @data[key]
    end

    # Sets the value of *key* to the given *value*.
    def []=(key : String, value : Type)
      @data[key] = value
    end

    # Returns the headers of the `Row`.
    def headers : Array(String)
      @data.keys
    end

    # Returns the values of the `Row` as an `Array` of type `Type`.
    def to_a : Array(Type)
      @data.values
    end

    # Returns the data of the `Row` as a `Hash`.
    def to_h : Hash(String, Type)
      @data
    end

    def to_s(io : IO) : Nil
      io << "Dataframe::Row{"
      @data.each_with_index do |(key, value), index|
        io << ", " if index > 0
        key.inspect(io)
        io << " => "
        value.inspect(io)
      end
      io << '}'
    end
  end
end
