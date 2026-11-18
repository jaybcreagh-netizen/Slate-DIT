# manifest.py
"""
Writes the proof of a copy next to the copy itself.

The proof is an ASC MHL history: an "ascmhl" folder at the top of the copied
card that lists every file with its checksum. Any tool that reads ASC MHL can
re-check the folder later without Slate. The files are written with the ASC's
own reference library (https://github.com/ascmitc/mhl) so the format is theirs,
not our reading of it.

The checksums are the ones Slate already worked out during the copy, so
writing the manifest does not read the footage again.
"""
import os
import platform
from datetime import datetime

from copy_engine import hash_format

# ASC MHL can record these. Slate's copy offers the first two.
SUPPORTED_FORMATS = ("xxh64", "md5", "sha1", "xxh3", "xxh128")


class ManifestError(Exception):
    pass


def write_manifest(root, entries, checksum_method, tool_name, tool_version, author=None):
    """
    Adds a new generation to the ASC MHL history at `root`.

    entries: list of (absolute file path, size in bytes, checksum as hex text)
    Returns the path of the .mhl file that was written, or None if there was
    nothing to record. Raises ManifestError if it could not be written, or if
    a file's checksum disagrees with an earlier generation already at `root`.
    """
    try:
        from ascmhl import utils as mhl_utils
        from ascmhl.generator import MHLGenerationCreationSession
        from ascmhl.hashlist import MHLAuthor, MHLCreatorInfo, MHLProcess, MHLProcessInfo, MHLTool
        from ascmhl.history import MHLHistory
        from ascmhl.ignore import MHLIgnoreSpec
    except ImportError as e:
        raise ManifestError(f"The 'ascmhl' package is not installed ({e}).") from e

    fmt = hash_format(checksum_method)
    if fmt not in SUPPORTED_FORMATS:
        raise ManifestError(f"Checksum type '{checksum_method}' cannot be recorded in ASC MHL.")

    root = os.path.abspath(root)
    try:
        history = MHLHistory.load_from_path(root)
        ignore_spec = MHLIgnoreSpec(history.latest_ignore_patterns())
        ignored = ignore_spec.get_path_spec()
        session = MHLGenerationCreationSession(history, ignore_spec)

        recorded = 0
        disagreements = []
        now = datetime.now()
        for path, size, checksum in entries:
            relative = os.path.relpath(path, root)
            if ignored.match_file(relative):
                continue  # e.g. .DS_Store, or an ascmhl folder that came from the card
            modified = datetime.fromtimestamp(os.path.getmtime(path))
            if not session.append_file_hash(path, size, modified, fmt, checksum, hash_date=now):
                disagreements.append(relative)
            recorded += 1
        if recorded == 0:
            return None

        creator = MHLCreatorInfo()
        creator.tool = MHLTool(tool_name, tool_version)
        creator.creation_date = mhl_utils.datetime_now_isostring()
        creator.host_name = platform.node()
        if author:
            creator.authors.append(MHLAuthor(author, role="DIT"))
        process = MHLProcessInfo()
        process.process = MHLProcess("transfer")
        session.commit(creator, process)
        written = session.new_hash_lists[history].file_path
    except ManifestError:
        raise
    except Exception as e:  # noqa: BLE001 - anything the library raises becomes one clear message
        raise ManifestError(f"Could not write the manifest in '{root}': {e}") from e

    if disagreements:
        shown = ", ".join(disagreements[:5]) + ("..." if len(disagreements) > 5 else "")
        raise ManifestError(
            f"{len(disagreements)} file(s) in '{root}' do not match the checksum recorded "
            f"in an earlier manifest: {shown}"
        )
    return written
