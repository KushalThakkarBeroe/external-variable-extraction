"""
Atomic file writes.

Several persisted artifacts here (the registry CSV, run_manifest.json,
tracker workbooks, the run report, success_flags.xlsx) are read-modify-
written repeatedly across runs, and nothing else depends on their being
readable mid-write. Before this module existed, every one of them was
written directly to its final path — a process killed mid-write (Ctrl+C,
crash, OOM) left a truncated or corrupt file in place, silently breaking
every subsequent run that reads it back.

write_atomic() has the caller build the file in a sibling temp path, then
swaps it into place with os.replace(), which is atomic on both POSIX and
NTFS: a reader never observes a partial write, and a kill mid-write leaves
an orphaned .tmp file behind instead of corrupting the real target.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Callable

log = logging.getLogger(__name__)


def write_atomic(path: Path | str, write_fn: Callable[[Path], None]) -> None:
    """
    Call write_fn(tmp_path) to produce the complete file contents at a
    temporary path, then atomically swap it into place at `path`.

    write_fn must write a whole, valid file to the path it's given (e.g.
    tmp_path.write_text(...), df.to_csv(tmp_path), an ExcelWriter opened
    against tmp_path). The temp file is created in the same directory as
    the final path so os.replace is a same-filesystem rename, not a
    cross-device copy (which would not be atomic).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        write_fn(tmp_path)
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
