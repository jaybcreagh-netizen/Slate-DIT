# copy_engine.py
"""
The part of Slate that copies one file and proves the copy is good.

It has no Qt in it, so it can be tested and timed on its own.

Rules it keeps:
  * A file only gets its real name at the destination after it has passed
    its check. While it is being written it is called "<name>.slate-partial".
  * A file that already exists at the destination is never overwritten. It is
    checked against the source instead.
  * Every destination is written by its own thread, so a slow drive does not
    hold up a fast one, and one failed drive does not stop the others.
  * In "full" mode the copy is read back from the drive (not from memory)
    and its checksum is compared with the checksum of what was read from
    the source.
"""
import hashlib
import os
import queue
import shutil
import sys
import threading
import time

import xxhash

CHUNK_SIZE = 4 * 1024 * 1024
PARTIAL_SUFFIX = ".slate-partial"
_QUEUE_DEPTH = 4  # chunks waiting per destination (16 MB) before the reader waits

EXISTS_DIFFERENT = "Not copied: a different file already exists at the destination"


class CopyCancelled(Exception):
    """Raised when the user cancels in the middle of a file."""


class Control:
    """Lets the caller pause or cancel a copy that is under way."""

    def __init__(self, is_paused=lambda: False, is_cancelled=lambda: False):
        self.is_paused = is_paused
        self.is_cancelled = is_cancelled

    def checkpoint(self):
        while self.is_paused() and not self.is_cancelled():
            time.sleep(0.2)
        if self.is_cancelled():
            raise CopyCancelled("Copy cancelled by user")


def hash_format(method):
    """Maps Slate's menu labels and MHL tag names to one short name."""
    name = (method or "").lower()
    if "md5" in name:
        return "md5"
    if "sha1" in name:
        return "sha1"
    if "128" in name:
        return "xxh128"
    if "xxh3" in name:
        return "xxh3"
    return "xxh64"


_HASHERS = {
    "md5": hashlib.md5,
    "sha1": hashlib.sha1,
    "xxh64": xxhash.xxh64,
    "xxh3": xxhash.xxh3_64,
    "xxh128": xxhash.xxh3_128,
}


def new_hasher(method):
    return _HASHERS[hash_format(method)]()


def _set_nocache(fd):
    """macOS: keep this file's data out of the memory cache."""
    if sys.platform == "darwin":
        try:
            import fcntl
            fcntl.fcntl(fd, fcntl.F_NOCACHE, 1)
        except (ImportError, AttributeError, OSError):
            pass


def _drop_cache(fd):
    """Linux: forget any copy of this file held in memory."""
    if hasattr(os, "posix_fadvise"):
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        except OSError:
            pass


def _open_source(path):
    return open(path, "rb", buffering=0)


def _open_dest(path):
    f = open(path, "wb")
    _set_nocache(f.fileno())
    return f


def _open_readback(path):
    f = open(path, "rb", buffering=0)
    _set_nocache(f.fileno())
    _drop_cache(f.fileno())
    return f


def hash_file(path, method, control=None):
    """Checksum of a file, read from the drive rather than from memory."""
    hasher = new_hasher(method)
    buf = bytearray(CHUNK_SIZE)
    view = memoryview(buf)
    with _open_readback(path) as f:
        while True:
            if control:
                control.checkpoint()
            n = f.readinto(buf)
            if not n:
                break
            hasher.update(view[:n])
    return hasher.hexdigest()


def _remove_quietly(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _describe(error):
    if isinstance(error, OSError) and error.strerror:
        return error.strerror
    return str(error) or error.__class__.__name__


class _DestWriter(threading.Thread):
    """Writes one destination's copy of the file, under its temporary name."""

    def __init__(self, final_path):
        super().__init__(daemon=True)
        self.final_path = final_path
        self.tmp_path = final_path + PARTIAL_SUFFIX
        self.chunks = queue.Queue(maxsize=_QUEUE_DEPTH)
        self.error = None

    def _write(self, chunks):
        f = None
        try:
            os.makedirs(os.path.dirname(self.final_path) or ".", exist_ok=True)
            f = _open_dest(self.tmp_path)
            for buf in chunks:
                f.write(buf)
            f.flush()
            os.fsync(f.fileno())
        except BaseException as e:  # noqa: BLE001 - reported to the caller, never swallowed
            self.error = e
        finally:
            if f is not None:
                try:
                    f.close()
                except OSError as e:
                    if self.error is None:
                        self.error = e

    def _from_queue(self):
        while True:
            buf = self.chunks.get()
            if buf is None:
                self._ended = True
                return
            yield buf

    def run(self):
        """Large files: take chunks from the reader as they arrive."""
        self._ended = False
        self._write(self._from_queue())
        # After an error, keep taking chunks so the reader is never left waiting on us.
        while not self._ended:
            self._ended = self.chunks.get() is None

    def write_now(self, chunks):
        """Small files: write on the calling thread. Starting a thread per
        file costs more than the write itself when there are thousands."""
        self._write(iter(chunks))


def copy_file(src, dest_paths, method, mode="full", control=None, on_progress=None):
    """
    Copies `src` to every path in `dest_paths` and checks each copy.

    mode is "full" (checksum), "size" (file size only) or "none" (no check).

    Returns {'size', 'checksum', 'destinations'} where each destination is
    {'path', 'ok', 'verified', 'status', 'action'}. `ok` is False for any
    destination that did not end up with a good copy.

    Raises CopyCancelled if cancelled, or OSError if the source cannot be
    read. In both cases no partly written file is left behind.
    """
    control = control or Control()
    src_size = os.path.getsize(src)
    results = [{'path': p, 'ok': False, 'verified': False, 'status': '', 'action': 'failed'} for p in dest_paths]

    writers = {}
    existing = []
    for i, path in enumerate(dest_paths):
        if os.path.lexists(path):
            existing.append(i)
        else:
            writers[i] = _DestWriter(path)

    hasher = new_hasher(method) if mode == "full" else None
    same_size_existing = [i for i in existing if os.path.isfile(dest_paths[i]) and os.path.getsize(dest_paths[i]) == src_size]
    need_source_hash = mode == "full" and bool(same_size_existing)
    bytes_read = 0
    read_complete = False
    # A file that fits in one chunk is handled on this thread, start to finish.
    threaded = src_size > CHUNK_SIZE
    held = []

    if threaded:
        for w in writers.values():
            w.start()
    try:
        try:
            if writers or need_source_hash:
                with _open_source(src) as fsrc:
                    while True:
                        control.checkpoint()
                        buf = fsrc.read(CHUNK_SIZE)
                        if not buf:
                            read_complete = True
                            break
                        if hasher:
                            hasher.update(buf)
                        any_alive = not threaded
                        if threaded:
                            for w in writers.values():
                                if w.error is None:
                                    w.chunks.put(buf)
                                    any_alive = True
                        else:
                            held.append(buf)
                        bytes_read += len(buf)
                        if on_progress:
                            on_progress(bytes_read, src_size)
                        if not any_alive and not need_source_hash:
                            break  # every destination has failed; nothing left to read for
            else:
                bytes_read = src_size
                read_complete = True
        finally:
            if threaded:
                for w in writers.values():
                    w.chunks.put(None)
                for w in writers.values():
                    w.join()
        if not threaded:
            for w in writers.values():
                w.write_now(held)
    except BaseException:
        for w in writers.values():
            _remove_quietly(w.tmp_path)
        raise

    size = bytes_read
    checksum = hasher.hexdigest() if hasher and read_complete else ""
    cancelled = []

    def finish_written(i, w):
        result = results[i]
        try:
            if w.error is not None:
                result['status'] = f"Write failed: {_describe(w.error)}"
                return
            if os.path.getsize(w.tmp_path) != size:
                result['status'] = "Size mismatch"
                return
            if mode == "full" and hash_file(w.tmp_path, method, control) != checksum:
                result['status'] = "Verification FAILED (checksum mismatch)"
                return
            os.replace(w.tmp_path, w.final_path)
            try:
                shutil.copystat(src, w.final_path)
            except OSError:
                pass  # the data is good; this drive just cannot hold the timestamps
            result['ok'] = True
            result['action'] = 'copied'
            result['verified'] = mode in ("full", "size")
            result['status'] = {"full": "Verified", "size": "Verified (Size Only)"}.get(mode, "Unverified")
        except CopyCancelled as e:
            cancelled.append(e)
        except OSError as e:
            result['status'] = f"Write failed: {_describe(e)}"
        finally:
            if not result['ok']:
                _remove_quietly(w.tmp_path)

    def check_existing(i):
        result = results[i]
        path = dest_paths[i]
        try:
            if i not in same_size_existing:
                result['status'] = EXISTS_DIFFERENT
            elif mode == "full":
                if hash_file(path, method, control) == checksum:
                    result.update(ok=True, verified=True, action='existing', status="Verified (already at destination)")
                else:
                    result['status'] = EXISTS_DIFFERENT
            elif mode == "size":
                result.update(ok=True, verified=True, action='existing', status="Verified (Size Only, already at destination)")
            else:
                result.update(ok=True, action='existing', status="Already at destination (unverified)")
        except CopyCancelled as e:
            cancelled.append(e)
        except OSError as e:
            result['status'] = f"Could not read the existing file: {_describe(e)}"

    if threaded:
        jobs = [threading.Thread(target=finish_written, args=(i, w), daemon=True) for i, w in writers.items()]
        jobs += [threading.Thread(target=check_existing, args=(i,), daemon=True) for i in existing]
        for t in jobs:
            t.start()
        for t in jobs:
            t.join()
    else:
        for i, w in writers.items():
            finish_written(i, w)
        for i in existing:
            check_existing(i)
    if cancelled:
        raise cancelled[0]

    return {'size': size, 'checksum': checksum, 'destinations': results}


def find_path_conflict(sources, destinations):
    """
    Returns a sentence describing the problem if a destination is the same
    folder as a source, or sits inside one (or the other way round).
    Copying in that situation would read its own output. Returns None if fine.
    """
    def norm(p):
        return os.path.normcase(os.path.realpath(p)).rstrip(os.sep) + os.sep

    for s in sources:
        for d in destinations:
            ns, nd = norm(s), norm(d)
            if ns == nd:
                return f"'{d}' is both a source and a destination."
            if nd.startswith(ns):
                return f"The destination '{d}' is inside the source '{s}'."
            if ns.startswith(nd):
                return f"The source '{s}' is inside the destination '{d}'."
    return None
