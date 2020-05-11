# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from unittest.mock import MagicMock

import pytest

from rpm_s3_mirror.mirror import InvalidSnapshotID, Mirror


def test_mirror_rejects_invalid_snapshot_id(mirror_config):
    mirror = Mirror(config=mirror_config)
    mirror.s3 = MagicMock()
    with pytest.raises(InvalidSnapshotID):
        mirror.snapshot(snapshot_id="not-valid-with!-this+")
    with pytest.raises(InvalidSnapshotID):
        mirror.snapshot(snapshot_id="trailingnewline\n")
    assert not mirror.s3.exists.called
