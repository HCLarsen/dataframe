class Dataframe
  class Column(T)
    include Indexable::Mutable(T)

    @data = Array(T).new

    delegate :size, to: @data

    # Creates an empty `Column`.
    def initialize
      if !Dataframe::TYPES.includes?(T)
        raise InvalidTypeError.new(T)
      end
    end

    # Creates a `Column` from the values in **new_data**.
    def initialize(new_data : Array(Type))
      @data = new_data.map { |e| e.as(T) }
    end

    def [](index : Int32)
      @data[index]
    end

    def sum : T
      perform_numeric_operation do
        @data.sum
      end
    end

    private def perform_numeric_operation(&)
      {% if T == Int32 || T == Float64 %}
        yield
      {% else %}
        raise NonNumericTypeError.new(T)
      {% end %}
    end

    def avg : Float64
      perform_numeric_operation do
        sum / @data.size
      end
    end

    def mode : Array(T)
      frequency = freq
      max = freq.values.max

      frequency.select { |k, v| v == max }.map { |k, v| k }
    end

    def to_a : Array(T)
      @data
    end

    def to_s(io : IO) : Nil
      io << "Dataframe::Column{"
      join io, ", ", &.inspect(io)
      io << '}'
    end

    @[AlwaysInline]
    def unsafe_fetch(index : Int) : T
      @data[index]
    end

    @[AlwaysInline]
    def unsafe_put(index : Int, value : T)
      @data[index] = value
    end

    private def freq : Hash(T, Int32)
      frequency = Hash(T, Int32).new(0)
      @data.each do |e|
        frequency[e] += 1
      end

      return frequency
    end
  end
end
