# frozen_string_literal: true

class WaterDropMiddlewareNoMiddlewaresTest < WaterDropTest::Base
  def setup
    @middleware = WaterDrop::Middleware.new
    @message = build(:valid_message)
  end

  def test_run_does_not_change_message
    message_before = @message.dup
    @middleware.run(@message)

    assert_equal message_before, @message
  end
end

class WaterDropMiddlewareMorphingMiddlewareTest < WaterDropTest::Base
  def setup
    @middleware = WaterDrop::Middleware.new
    @message = build(:valid_message)

    mid1 = lambda do |msg|
      msg[:test] = 1
      msg
    end

    @middleware.prepend mid1
  end

  def test_run_changes_message_test_key
    assert_nil @message[:test]
    @middleware.run(@message)

    assert_equal 1, @message[:test]
  end
end

class WaterDropMiddlewareMorphingMiddlewaresTest < WaterDropTest::Base
  def setup
    @middleware = WaterDrop::Middleware.new
    @message = build(:valid_message)

    mid1 = lambda do |msg|
      msg[:test] = 1
      msg
    end

    mid2 = lambda do |msg|
      msg[:test2] = 2
      msg
    end

    @middleware.prepend mid1
    @middleware.prepend mid2
  end

  def test_run_changes_message_test_key
    assert_nil @message[:test]
    @middleware.run(@message)

    assert_equal 1, @message[:test]
  end

  def test_run_changes_message_test2_key
    assert_nil @message[:test2]
    @middleware.run(@message)

    assert_equal 2, @message[:test2]
  end
end

class WaterDropMiddlewareNonMorphingMiddlewareTest < WaterDropTest::Base
  def setup
    @middleware = WaterDrop::Middleware.new
    @message = build(:valid_message)

    mid1 = lambda do |msg|
      msg = msg.dup
      msg[:test] = 1
      msg
    end

    @middleware.prepend mid1
  end

  def test_run_does_not_change_original_message
    message_before = @message.dup
    @middleware.run(@message)

    assert_equal message_before, @message
  end

  def test_run_returns_modified_message
    assert_equal 1, @middleware.run(@message)[:test]
  end
end

class WaterDropMiddlewareNonMorphingMiddlewaresTest < WaterDropTest::Base
  def setup
    @middleware = WaterDrop::Middleware.new
    @message = build(:valid_message)

    mid1 = lambda do |msg|
      msg = msg.dup
      msg[:test] = 1
      msg
    end

    mid2 = lambda do |msg|
      msg = msg.dup
      msg[:test2] = 2
      msg
    end

    @middleware.prepend mid1
    @middleware.prepend mid2
  end

  def test_run_does_not_change_original_message
    message_before = @message.dup
    @middleware.run(@message)

    assert_equal message_before, @message
  end

  def test_run_returns_message_with_test_key
    assert_equal 1, @middleware.run(@message)[:test]
  end

  def test_run_returns_message_with_test2_key
    assert_equal 2, @middleware.run(@message)[:test2]
  end
end

class WaterDropMiddlewareMorphingMiddlewareOnManyTest < WaterDropTest::Base
  def setup
    @middleware = WaterDrop::Middleware.new
    @message = build(:valid_message)

    mid1 = lambda do |msg|
      msg[:test] = 1
      msg
    end

    @middleware.append mid1
  end

  def test_run_many_changes_message_test_key
    assert_nil @message[:test]
    @middleware.run_many([@message])

    assert_equal 1, @message[:test]
  end
end

class WaterDropMiddlewareMorphingMiddlewaresOnManyTest < WaterDropTest::Base
  def setup
    @middleware = WaterDrop::Middleware.new
    @message = build(:valid_message)

    mid1 = lambda do |msg|
      msg[:test] = 1
      msg
    end

    mid2 = lambda do |msg|
      msg[:test2] = 2
      msg
    end

    @middleware.append mid1
    @middleware.append mid2
  end

  def test_run_many_changes_message_test_key
    assert_nil @message[:test]
    @middleware.run_many([@message])

    assert_equal 1, @message[:test]
  end

  def test_run_many_changes_message_test2_key
    assert_nil @message[:test2]
    @middleware.run_many([@message])

    assert_equal 2, @message[:test2]
  end
end

class WaterDropMiddlewareNonMorphingMiddlewareOnManyTest < WaterDropTest::Base
  def setup
    @middleware = WaterDrop::Middleware.new
    @message = build(:valid_message)

    mid1 = lambda do |msg|
      msg = msg.dup
      msg[:test] = 1
      msg
    end

    @middleware.append mid1
  end

  def test_run_many_does_not_change_original_message
    message_before = @message.dup
    @middleware.run_many([@message])

    assert_equal message_before, @message
  end

  def test_run_many_returns_modified_message
    assert_equal 1, @middleware.run_many([@message]).first[:test]
  end
end
