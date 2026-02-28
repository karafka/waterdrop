# frozen_string_literal: true

class WaterDropErrorsBaseErrorTest < WaterDropTest::Base
  def test_base_error_inherits_from_standard_error
    assert_operator WaterDrop::Errors::BaseError, :<, StandardError
  end
end

class WaterDropErrorsConfigurationInvalidErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::ConfigurationInvalidError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsVariantInvalidErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::VariantInvalidError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsProducerNotConfiguredErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::ProducerNotConfiguredError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsProducerAlreadyConfiguredErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::ProducerAlreadyConfiguredError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsProducerUsedInParentProcessTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::ProducerUsedInParentProcess, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsProducerClosedErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::ProducerClosedError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsProducerTransactionalCloseAttemptErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::ProducerTransactionalCloseAttemptError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsMessageInvalidErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::MessageInvalidError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsStatusInvalidErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::StatusInvalidError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsAbortTransactionTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::AbortTransaction, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsTransactionRequiredErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::TransactionRequiredError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropErrorsPollerErrorTest < WaterDropTest::Base
  def test_inherits_from_base_error
    assert_operator WaterDrop::Errors::PollerError, :<, WaterDrop::Errors::BaseError
  end
end

class WaterDropAbortTransactionAliasTest < WaterDropTest::Base
  def test_root_abort_transaction_is_alias_for_errors_abort_transaction
    assert_equal WaterDrop::Errors::AbortTransaction, WaterDrop::AbortTransaction
  end
end
