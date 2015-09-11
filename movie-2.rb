require 'rubygems'
require 'sqlite3'
require 'set'
include Math

class Item_Prediction
  # a simple structure to store the 4 values
  attr_accessor(:user_id, :item_id, :rate, :predicted_rate)
  
  def initialize(id,movie,rate,predicted_rate)
    @user_id = id
    @item_id = movie
    @rate = rate
    @predicted_rate = predicted_rate
  end
end

class MovieTest
  attr_accessor(:list_of_items)
  def initialize(movie_data ,number_of_test_cases)
    working_data_base = movie_data.test_data
    # remove < 20
    rt = working_data_base.execute("SELECT user_id, item_id, rating FROM movie_table")
    @list_of_items = Array.new
    puts rt.count
    count = 0
    
    rt.each do |user_item_rate|
      user_id = user_item_rate[0]
      item_id = user_item_rate[1]
      rating = user_item_rate[2]
      predicted_rate = movie_data.predict(user_id, item_id)
      puts predicted_rate
      item = Item_Prediction.new(user_id, item_id, rating, predicted_rate)
      @list_of_items << item
      
      count += 1
      if count > number_of_test_cases
        break
      end
    end
  end
  
  def mean
    #average error
    puts("@list_of_items: #{(@list_of_items.first.predicted_rate - @list_of_items.first.rate).abs}")
    sum = @list_of_items.inject(0.0){ |accum, i| accum + (i.rate - i.predicted_rate).abs }
    puts("sum: #{sum}")
    return sum / @list_of_items.count
  end
  
  def stddev
    # stndard deviation of the error
    m = self.mean
    sum = @list_of_items.inject(0){|accum, i| accum + (i.predicted_rate - m)**2 }
    return sqrt(sum/(@list_of_items.length - 1).to_f)
  end
  
  def rms
    # root mean sqrt eroor
    sum = @list_of_items.inject(0){|accum, i| accum + (i.predicted_rate - i.rate)**2 }
    return sqrt(sum/(@list_of_items.length).to_f)
  end
  
  def to_a
    # an array of Item_Prediction
    return @list_of_items
  end
end

class MovieData
  attr_accessor(:base_data , :test_data, :has_test_data)
  def initialize(data, u = :all, load_from_Data = :no)
    if load_from_Data == :no
      puts "loading from SQL"
      if (u == :all)
        @base_data = SQLite3::Database.new("#{u.to_s}.sqlite")
        @has_test_data = false
      else
      @base_data = SQLite3::Database.new("#{u.to_s}.sqlite")
      @test_data = SQLite3::Database.new("#{u.to_s}.test.sqlite")
      @has_test_data = true
    end
    else 
      if (u == :all)
        path = File.join(data, "u.data")
        path_test = :nil
        @has_test_data = false
      else
        path = File.join(data, "#{u.to_s}.base")
        path_test = File.join(data, "#{u.to_s}.test")
        @has_test_data = true
      end
      @path = path
      @path_test = path_test
      @base_data = loadDataFromPath(@path, u, test = :no)
      if path_test != :nil
        @test_data = loadDataFromPath(@path_test, u, test = :yes)
      end
    end
  end
  
  
  def loadDataFromPath(addr, u, test = :no)
    #load data from sql table
    if test == :no
      dbname = "#{u.to_s}.sqlite"
    else
      dbname = "#{u.to_s}.test.sqlite"
    end
    puts "loading from #{dbname}"
    File.delete(dbname) if File.exists? dbname
    db = SQLite3::Database.new( dbname )
    db.execute("CREATE TABLE movie_table(user_id, item_id, rating, date)")
    insert_query = "INSERT INTO movie_table(user_id, item_id, rating, date) VALUES(?, ?, ?, ?)"
    f = File.open(addr, "r")
    f.each_line do |line|
      lst = line.split("\t", 4)
      db.execute(insert_query, lst[0].to_i, lst[1].to_i, lst[2].to_i, lst[3].to_i)
    end
    f.close
    return db
  end
  
  def rating(user_id,item_id)
    #return user_id's rating on item_id
    rate = @base_data.execute("SELECT rating FROM movie_table WHERE user_id = #{user_id} AND item_id = #{item_id}")
    return rate
  end
  
  def predict(user_id,item_id)
    # predict how would user_id rate item_id
    rate = @base_data.execute("SELECT rating FROM movie_table WHERE item_id = #{item_id}")
    average = rate.inject(0.0){ |sum, n| sum + n[0] } / rate.size
    # puts "the ave predicted for user #{user_id} movie #{item_id} is #{average}"
    if average.nan?
      average = 3
    end
    return average
  end
  
  
  def viewers(item_id) 
    # returns the array of users that have seen item_id
    viewers = @base_data.execute("SELECT user_id FROM movie_table WHERE item_id = #{item_id}")
    arr = Array.new
    viewers = viewers.reduce{|arr, each_movie| arr.push(each_movie[0])}
    return viewers
  end
  
  def run_test(number_of_test_cases)
    #run test on base data against test data
    if @has_test_data
      puts "running tests"
      return MovieTest.new(self, number_of_test_cases)
    else
      puts "no test data available"
    end
  end
end

z = MovieData.new("ml-100k", u = :u1)
puts z.predict(13,857)
w = z.run_test(500)
puts w.mean
puts w.stddev
puts w.rms










