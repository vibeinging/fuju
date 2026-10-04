import tempfile
from pathlib import Path

import pytest
from yitrace_db import YiTraceDB


def test_flush_and_close_report_io_errors_and_leave_handle_retryable():
    with tempfile.TemporaryDirectory(prefix="yt-python-durability-") as tmp:
        db = YiTraceDB.open(tmp)
        db.ingest([dict(trace_id="durable-run", span_id="durable-span", ts=1, seq=1,
                        event_type=1, ext_span_id="durable-span", span_name="preserve acknowledged row")])
        for number in (1, 2):
            (Path(tmp) / "segments" / f"seg-{number}.tmp").mkdir()
        with pytest.raises(RuntimeError, match="flush yiTrace failed"):
            db.flush()
        with pytest.raises(RuntimeError, match="flush yiTrace failed"):
            db.close()
        assert "preserve acknowledged row" in str(db.trace("durable-run"))
        for number in (1, 2):
            (Path(tmp) / "segments" / f"seg-{number}.tmp").rmdir()
        db.close()
        with YiTraceDB.open(tmp) as reopened:
            assert "preserve acknowledged row" in str(reopened.trace("durable-run"))
