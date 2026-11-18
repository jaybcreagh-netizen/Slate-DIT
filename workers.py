# workers.py
import os
import time
import json
import platform
import subprocess
import xxhash
from xml.etree import ElementTree as ET
from datetime import datetime
from fractions import Fraction

import cv2
import psutil
from PySide6.QtCore import QThread, Signal


from PIL import Image as PILImage
import rawpy

import copy_engine
import manifest
from config import APP_NAME, APP_VERSION, FFMPEG_PATH, FFPROBE_PATH
from utils import check_command, resolve_path_template

class ScanWorker(QThread):
    scan_finished = Signal(dict)
    scan_progress = Signal(int, int) # files_found, current_total_size

    def __init__(self, job_params, file_queue, parent=None):
        super().__init__(parent)
        self.job_params = job_params
        self.file_queue = file_queue

    def _dest_root(self, dest_root, source_path):
        """The folder at this destination that will hold this source's files."""
        source_folder_name = os.path.basename(source_path.rstrip(os.path.sep))
        if self.job_params['has_template']:
            return os.path.join(dest_root, resolve_path_template(
                self.job_params['naming_preset']['template'],
                self.job_params['naming_preset'],
                self.job_params['card_counter'],
                source_folder_name
            ))
        if self.job_params['create_source_folder']:
            return os.path.join(dest_root, source_folder_name)
        return dest_root

    def _report_problem(self, message):
        # Anything the scan could not read is passed to the copy as a problem,
        # so the job cannot finish green with files missing.
        self.file_queue.put({'scan_error': message})

    def run(self):
        sources = self.job_params['sources']
        destinations = self.job_params['destinations']
        all_source_files = {}
        total_size = 0
        resolved_dests = {}
        files_found = 0

        try:
            for source_path in sources:
                # Worked out once per source, so a copy that runs past midnight
                # does not split one card across two dated folders.
                dest_roots = [self._dest_root(d, source_path) for d in destinations]

                def on_walk_error(error, source_path=source_path):
                    self._report_problem(f"Could not read '{error.filename or source_path}': {error.strerror or error}")

                for root, _, files in os.walk(source_path, onerror=on_walk_error):
                    for file in files:
                        full_path = os.path.join(root, file)
                        try:
                            file_size = os.path.getsize(full_path)
                        except OSError as e:
                            self._report_problem(f"Could not read '{full_path}': {e.strerror or e}")
                            continue
                        all_source_files[full_path] = source_path
                        total_size += file_size

                        files_found += 1
                        relative_path = os.path.relpath(full_path, source_path)
                        file_dests = [os.path.join(dest_root, relative_path) for dest_root in dest_roots]
                        resolved_dests[full_path] = file_dests

                        # Push to queue for immediate processing
                        task = {
                            'source': full_path,
                            'destinations': file_dests,
                            'dest_roots': dest_roots,
                            'size': file_size,
                            'base_source_path': source_path
                        }
                        self.file_queue.put(task)

                        if files_found % 10 == 0:
                            self.scan_progress.emit(files_found, total_size)
        except Exception as e:
            self._report_problem(f"The scan stopped early: {e}")
        finally:
            self.file_queue.put(None) # Sentinel

        self.job_params['all_source_files'] = all_source_files
        self.job_params['total_size'] = total_size
        self.job_params['resolved_dests'] = resolved_dests
        self.scan_finished.emit(self.job_params)

class EjectWorker(QThread):
    ejection_finished = Signal(str, bool)
    def __init__(self, path_to_eject, parent=None):
        super().__init__(parent)
        self.mount_path = path_to_eject
    def run(self):
        try:
            device_path = next((p.device for p in psutil.disk_partitions() if p.mountpoint == self.mount_path), None)
            if not device_path:
                self.ejection_finished.emit(self.mount_path, False)
                return
            system = platform.system()
            if system == "Darwin":
                try:
                    subprocess.run(["diskutil", "eject", device_path], check=True, capture_output=True)
                except subprocess.CalledProcessError:
                    subprocess.run(["diskutil", "unmountDisk", "force", device_path], check=True, capture_output=True)
            elif system == "Linux":
                subprocess.run(["eject", device_path], check=True, capture_output=True)
            elif system == "Windows":
                drive_letter = os.path.splitdrive(self.mount_path)[0]
                command = ["powershell", f"(New-Object -comObject Shell.Application).Namespace(17).ParseName('{drive_letter}').InvokeVerb('Eject')"]
                subprocess.run(command, check=True, capture_output=True)
            else:
                self.ejection_finished.emit(self.mount_path, False)
                return
            self.ejection_finished.emit(self.mount_path, True)
        except Exception:
            self.ejection_finished.emit(self.mount_path, False)

class PostProcessWorker(QThread):
    progress = Signal(int, int, str)
    file_processed = Signal(str, str, dict)
    job_processed = Signal(str)
    def __init__(self, job, project_path, parent=None):
        super().__init__(parent)
        self.job = job
        self.project_path = project_path
        self._ffprobe_available = check_command(FFPROBE_PATH)
        self._ffmpeg_available = check_command(FFMPEG_PATH)
    def run(self):
        if not self.job or 'report' not in self.job or 'files' not in self.job['report']:
            if self.job:
                self.job_processed.emit(self.job.get('id', ''))
            return
        files_to_process = self.job['report']['files']
        total_files = len(files_to_process)
        job_id = self.job['id']
        for i, file_info in enumerate(files_to_process):
            self.progress.emit(i + 1, total_files, os.path.basename(file_info['source']))
            updates = {}
            verified_dest = next((d['path'] for d in file_info['destinations'] if d.get('verified')), None)
            if not verified_dest:
                continue
            if self._is_video_file(file_info['source']):
                if self._ffprobe_available:
                    updates['metadata'] = self._get_video_metadata(verified_dest)
                updates['thumbnail'] = self._create_video_thumbnail_robust(verified_dest)
            elif self._is_image_file(file_info['source']):
                updates['thumbnail'] = self._create_image_thumbnail(verified_dest)
            if updates:
                self.file_processed.emit(job_id, file_info['source'], updates)
        self.job_processed.emit(job_id)
    def _is_video_file(self, file_path):
        return any(file_path.lower().endswith(ext) for ext in ['.mov', '.mp4', '.mxf', '.avi', '.r3d', '.braw'])
    def _is_image_file(self, file_path):
        return any(file_path.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.tif', '.tiff', '.png', '.dng', '.cr2', '.cr3', '.nef', '.arw', '.rw2'])
    def _get_video_metadata(self, file_path):
        try:
            creationflags = subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0
            cmd = [FFPROBE_PATH, "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", file_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, creationflags=creationflags)
            metadata = json.loads(result.stdout)
            video_stream = next((s for s in metadata.get('streams', []) if s.get('codec_type') == 'video'), None)
            if not video_stream: return {}
            return {"format": metadata.get('format', {}).get('format_long_name', 'N/A'),
                    "codec": video_stream.get('codec_long_name', 'N/A'),
                    "resolution": f"{video_stream.get('width')}x{video_stream.get('height')}",
                    "fps": float(Fraction(video_stream.get('r_frame_rate') or '0/1')),
                    "duration": f"{float(video_stream.get('duration', 0)):.2f}s"}
        except Exception: return {}
    def _create_video_thumbnail_robust(self, video_path, thumb_size=(160, 90)):
        thumb_path = self._get_thumb_path(video_path)
        if os.path.exists(thumb_path): return thumb_path
        try:
            cap = cv2.VideoCapture(video_path)
            if cap.isOpened():
                frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
                frame_pos = int(frame_count * 0.1) if frame_count > 0 else 0
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
                ret, frame = cap.read()
                if ret:
                    resized_frame = cv2.resize(frame, thumb_size, interpolation=cv2.INTER_AREA)
                    cv2.imwrite(thumb_path, resized_frame)
                    cap.release()
                    return thumb_path
            cap.release()
        except Exception: pass
        if self._ffmpeg_available:
            try:
                creationflags = subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0
                cmd = [ FFMPEG_PATH, '-y', '-i', video_path, '-vf', f'scale={thumb_size[0]}:{thumb_size[1]}:force_original_aspect_ratio=decrease,pad={thumb_size[0]}:{thumb_size[1]}:(ow-iw)/2:(oh-ih)/2', '-ss', '00:00:01', '-vframes', '1', thumb_path ]
                subprocess.run(cmd, check=True, capture_output=True, creationflags=creationflags)
                return thumb_path if os.path.exists(thumb_path) else None
            except Exception: return None
        return None
    def _create_image_thumbnail(self, image_path, thumb_size=(160, 160)):
        thumb_path = self._get_thumb_path(image_path)
        if os.path.exists(thumb_path): return thumb_path
        try:
            img = None
            is_raw = any(image_path.lower().endswith(ext) for ext in ['.dng', '.cr2', '.cr3', '.nef', '.arw', '.rw2'])
            if is_raw:
                with rawpy.imread(image_path) as raw:
                    rgb = raw.postprocess(use_camera_wb=True, no_auto_bright=True)
                img = PILImage.fromarray(rgb)
            else:
                img = PILImage.open(image_path)
            img.thumbnail(thumb_size, PILImage.Resampling.LANCZOS)
            if img.mode not in ('RGB', 'RGBA'):
                img = img.convert('RGB')
            img.save(thumb_path, 'JPEG', quality=85)
            return thumb_path
        except Exception as e:
            print(f"Could not create thumbnail for {image_path}: {e}")
            return None
    def _get_thumb_path(self, file_path):
        temp_dir = os.path.join(self.project_path, ".dit_project", "thumbnails")
        os.makedirs(temp_dir, exist_ok=True)
        thumb_name = f"{os.path.basename(file_path)}_{xxhash.xxh64(file_path.encode()).hexdigest()}.jpg"
        return os.path.join(temp_dir, thumb_name)

class TransferWorker(QThread):
    progress = Signal(str, float, float, int)
    file_progress = Signal(int, str, str, float)
    job_finished = Signal(dict)
    error = Signal(str, str)
    def __init__(self, job, project_path, parent=None):
        super().__init__(parent)
        self.job = job; self.project_path = project_path
        self.is_paused = False; self.is_cancelled = False

    def run(self):
        checksum_method = self.job['checksum_method']
        verification_mode = self.job.get("verification_mode", "full")
        report_data = {
            'job_id': self.job['id'], 'start_time': datetime.now(),
            'sources': self.job['sources'], 'destinations': self.job['destinations'],
            'checksum_method': checksum_method, 'files': [],
            'status': 'Completed', 'total_size': 0, 'errors': [], 'manifests': []
        }

        total_bytes_processed_in_job = 0
        job_start_time = time.monotonic()
        file_queue = self.job['file_queue']
        control = copy_engine.Control(lambda: self.is_paused, lambda: self.is_cancelled)
        # destination folder -> the verified files in it, for the manifest
        verified_by_root = {}
        sources_with_problems = set()

        def fail(message, base_source_path=None):
            report_data['errors'].append(message)
            if base_source_path:
                sources_with_problems.add(base_source_path)
            self.error.emit(message, self.job['id'])

        def cancelled():
            report_data['status'] = 'Cancelled'
            report_data['end_time'] = datetime.now()
            report_data['total_size'] = total_bytes_processed_in_job
            self.job_finished.emit(report_data)

        while True:
            while self.is_paused:
                if self.is_cancelled: break
                time.sleep(0.5)
            if self.is_cancelled:
                cancelled()
                return

            task = file_queue.get()
            if task is None:
                file_queue.task_done()
                break # Sentinel received, all files processed

            if 'scan_error' in task:
                # The scan could not read part of a source. The job must not end green.
                fail(task['scan_error'])
                sources_with_problems.update(self.job['sources'])
                file_queue.task_done()
                continue

            source_file_path = task['source']
            dest_paths = task['destinations']
            dest_roots = task.get('dest_roots')
            base_source_path = task['base_source_path']
            name = os.path.basename(source_file_path)

            file_info = {
                'source': source_file_path, 'destinations': [], 'status': 'Failed',
                'checksum': '', 'size': task['size'],
                'custom_metadata': self.job['metadata'].get(base_source_path, {})
            }

            try:
                label = "Copying & Hashing..." if verification_mode == "full" else "Copying..."
                self.file_progress.emit(0, label, source_file_path, 0.0)

                def on_progress(done, total, path=source_file_path):
                    percent = int((done / total) * 100) if total > 0 else 100
                    self.file_progress.emit(percent, "Copying...", path, 0)

                result = copy_engine.copy_file(source_file_path, dest_paths, checksum_method,
                                               verification_mode, control, on_progress)
                file_info['size'] = result['size']
                file_info['checksum'] = result['checksum']
                file_info['destinations'] = result['destinations']

                failed = [d for d in result['destinations'] if not d['ok']]
                if failed:
                    details = "; ".join(f"{d['path']}: {d['status']}" for d in failed)
                    fail(f"{name} was NOT copied safely. {details}", base_source_path)
                elif verification_mode == "none":
                    file_info['status'] = 'Copied (Unverified)'
                else:
                    file_info['status'] = 'Verified'

                if verification_mode == "full" and dest_roots:
                    for dest, root in zip(result['destinations'], dest_roots):
                        if dest['verified']:
                            verified_by_root.setdefault(root, []).append((dest['path'], result['size'], result['checksum']))
                report_data['files'].append(file_info)

            except copy_engine.CopyCancelled:
                file_queue.task_done()
                cancelled()
                return
            except Exception as e:
                file_info['status'] = f'Error: {e}'; report_data['files'].append(file_info)
                fail(f"Error processing {name}: {e}", base_source_path)

            file_queue.task_done()

            total_bytes_processed_in_job += file_info.get('size', 0)
            elapsed_time = time.monotonic() - job_start_time
            speed_mbps = (total_bytes_processed_in_job / (1024*1024)) / elapsed_time if elapsed_time > 0 else 0
            self.progress.emit(self.job['id'], total_bytes_processed_in_job, speed_mbps, -1)

        if not report_data['files'] and not report_data['errors']:
            fail("No files were found to copy. Check that the source is connected and readable.")

        # Write the proof next to each copy: an ASC MHL manifest in every
        # destination folder, listing the files that passed.
        for root, entries in verified_by_root.items():
            record = {'root': root, 'path': None, 'files': len(entries)}
            try:
                record['path'] = manifest.write_manifest(
                    root, entries, checksum_method, APP_NAME, APP_VERSION, self.job.get('author'))
            except manifest.ManifestError as e:
                record['error'] = str(e)
                fail(f"Manifest problem: {e}")
            report_data['manifests'].append(record)

        if report_data['errors']:
            report_data['status'] = 'Completed with errors'
        elif self.job.get('eject_on_completion') and verification_mode != "none":
            # Only offer to eject cards whose every file was verified.
            report_data['ejectable_sources_on_success'] = [
                s for s in self.job['sources'] if s not in sources_with_problems and os.path.ismount(s)]
        report_data['end_time'] = datetime.now()
        report_data['total_size'] = total_bytes_processed_in_job
        self.job_finished.emit(report_data)

    def pause(self): self.is_paused = True
    def resume(self): self.is_paused = False
    def cancel(self): self.is_cancelled = True; self.is_paused = False

# Checksum tags Slate can check, best first. The value is the algorithm to run.
#   xxhash64be - older MHL files, digest written most significant byte first
#   xxh64 / xxh3 / xxh128 - ASC MHL names
#   xxhash64   - older MHL files, digest usually written the other way round
MHL_HASH_TAGS = {
    'xxhash64be': 'xxh64', 'xxh64': 'xxh64', 'xxh128': 'xxh128', 'xxh3': 'xxh3',
    'md5': 'md5', 'sha1': 'sha1', 'xxhash64': 'xxh64',
}


def parse_mhl(file_path):
    """
    Reads the file list out of an MHL file: either the older format
    (<file>, <size>, <md5>/<xxhash64be>) or ASC MHL (<path size=...>, <xxh64>).
    Returns a list of (relative path, expected checksum, tag name).
    The tag name is None when the entry only has a checksum type Slate cannot check.
    """
    def local(tag):
        return tag.rsplit('}', 1)[-1] if isinstance(tag, str) else ''

    files = []
    for elem in ET.parse(file_path).getroot().iter():
        if local(elem.tag) != 'hash':
            continue
        relative_path, found = None, {}
        for child in elem:
            tag = local(child.tag)
            if tag in ('file', 'path'):
                relative_path = child.text
            elif tag in MHL_HASH_TAGS and child.text:
                found[tag] = child.text.strip()
        if not relative_path:
            continue
        tag = next((t for t in MHL_HASH_TAGS if t in found), None)
        files.append((relative_path, found.get(tag), tag))
    return files


def mhl_hash_matches(tag, expected, actual):
    expected, actual = expected.lower(), actual.lower()
    if expected == actual:
        return True
    if tag == 'xxhash64':
        # Older tools wrote this tag with the bytes in the opposite order.
        try:
            return bytes.fromhex(expected)[::-1].hex() == actual
        except ValueError:
            return False
    return False


class MHLVerifyWorker(QThread):
    progress = Signal(int, str, float, int)
    file_progress = Signal(int, str, str, float)
    job_finished = Signal(dict)
    error = Signal(str, str)
    def __init__(self, job, parent=None):
        super().__init__(parent)
        self.job = job
        self.is_paused = False
        self.is_cancelled = False
    def run(self):
        mhl_file_path = self.job['mhl_file']
        target_dir = self.job['target_dir']
        report_data = {'job_id': self.job['id'], 'start_time': datetime.now(),
                       'mhl_file': mhl_file_path, 'target_dir': target_dir,
                       'files': [], 'status': 'Completed', 'errors': [],
                       'verified_count': 0, 'failed_count': 0, 'missing_count': 0}
        control = copy_engine.Control(lambda: self.is_paused, lambda: self.is_cancelled)
        try:
            self.progress.emit(-1, "Parsing MHL file...", 0.0, -1)
            files_to_verify = parse_mhl(mhl_file_path)
            total_files = len(files_to_verify)
            if total_files == 0:
                # Nothing listed means nothing was checked. That is not a pass.
                raise ValueError("No files are listed in this MHL file, so nothing could be checked.")
            for i, (relative_path, expected_hash, tag) in enumerate(files_to_verify):
                control.checkpoint()
                full_path = os.path.join(target_dir, relative_path)
                file_report = {'path': full_path, 'expected_hash': expected_hash, 'hash_type': tag}
                if not os.path.exists(full_path):
                    file_report['status'] = 'Missing'
                    report_data['missing_count'] += 1
                elif tag is None:
                    file_report.update(status='FAILED', expected_hash='(none Slate can check)',
                                       hash_type='unsupported', actual_hash='not checked')
                    report_data['failed_count'] += 1
                else:
                    self.file_progress.emit(0, f"Verifying {os.path.basename(full_path)}...", full_path, 0.0)
                    actual_hash = copy_engine.hash_file(full_path, MHL_HASH_TAGS[tag], control)
                    if mhl_hash_matches(tag, expected_hash, actual_hash):
                        file_report['status'] = 'Verified'
                        report_data['verified_count'] += 1
                    else:
                        file_report['status'] = 'FAILED'
                        file_report['actual_hash'] = actual_hash
                        report_data['failed_count'] += 1
                report_data['files'].append(file_report)
                progress_percent = int(((i + 1) / total_files) * 100) if total_files > 0 else 100
                progress_text = f"Verified file {i + 1} of {total_files}"
                self.progress.emit(progress_percent, progress_text, 0.0, -1)
        except copy_engine.CopyCancelled:
            report_data['status'] = 'Cancelled'
            report_data['end_time'] = datetime.now()
            self.job_finished.emit(report_data)
            return
        except Exception as e:
            error_msg = f"A critical error occurred: {e}"
            report_data['status'] = 'Failed'
            report_data['errors'].append(error_msg)
            self.error.emit(error_msg, self.job['id'])
        if report_data['status'] != 'Failed' and (report_data['failed_count'] > 0 or report_data['missing_count'] > 0):
            report_data['status'] = 'Completed with issues'
        report_data['end_time'] = datetime.now()
        self.job_finished.emit(report_data)
    def pause(self): self.is_paused = True
    def resume(self): self.is_paused = False
    def cancel(self): self.is_cancelled = True; self.is_paused = False

class ReportWorker(QThread):
    finished = Signal(bool, str, str)
    def __init__(self, report_generator_func, report_data, file_path, parent=None):
        super().__init__(parent)
        self.report_generator_func = report_generator_func
        self.report_data = report_data
        self.file_path = file_path
    def run(self):
        try:
            self.report_generator_func(self.report_data, self.file_path)
            self.finished.emit(True, self.file_path, "")
        except Exception as e:
            self.finished.emit(False, self.file_path, str(e))