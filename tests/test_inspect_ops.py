from swpt_creditors import inspect_ops
import pytest

D_ID = -1
C_ID = 4294967296


def test_forbidden_operations(app):
    app.config['APP_MAX_CREDITOR_RECONFIGS'] = 0
    app.config['APP_MAX_CREDITOR_TRANSFERS'] = 0
    app.config['APP_MAX_CREDITOR_ACCOUNTS'] = 0
    app.config['APP_MAX_CREDITOR_INITIATIONS'] = 0

    with pytest.raises(inspect_ops.ForbiddenOperation):
        inspect_ops.allow_account_creation(C_ID, D_ID)

    with pytest.raises(inspect_ops.ForbiddenOperation):
        inspect_ops.allow_transfer_creation(C_ID, D_ID)

    with pytest.raises(inspect_ops.ForbiddenOperation):
        inspect_ops.allow_account_reconfig(C_ID, D_ID)

    with pytest.raises(inspect_ops.ForbiddenOperation):
        inspect_ops._allow_transfer_initiation(C_ID, D_ID)
