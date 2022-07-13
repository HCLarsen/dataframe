class Dataframe
  class Row
    getter headers : Array(String)

    def initialize(@data : Array(String), @headers : Array(String))
    end

    def to_a : Array(String)
      @data
    end

    def to_h : Hash(String, String)
      Hash.zip(@headers, @data)
    end
  end
end
