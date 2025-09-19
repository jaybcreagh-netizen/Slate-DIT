# workers.py
import os
import shutil
import time
import json
import platform
import subprocess
import xxhash
import hashlib
from xml.etree import ElementTree as ET
from datetime import datetime

import cv2
import psutil
from PySide6.QtCore import QThread, Signal

from config import FFMPEG_PATH, FFPROBE_PATH
from utils import check_command, resolve_path_template
from models import Job, JobStatus

class ScanWorker(QThread):
    # This worker is already well-structured and does not need changes.
    scan_finished = Signal(dict)
    def __init__(self, job_params, parent=None):
        super().__init__(parent)
        self.job_params = job_params
    def run(self):
        sources = self.job_params['sources']
        destinations = self.job_params['destinations']
        total_size = 0
        resolved_dests = {}

        for source_path in sources:
            source_files_in_dir = []
            for root, _, files in os.walk(source_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    source_files_in_dir.append(full_path)
                    try: total_size += os.path.getsize(full_path)
                    except FileNotFoundError: continue
            
            for source_file in source_files_in_dir:
                resolved_dests[source_file] = []
                for dest_root in destinations:
                    if self.job_params['has_template']:
                        resolved_path = resolve_path_template(
                            self.job_params['naming_preset']['template'],
                            self.job_params['naming_preset'],
                            self.job_params['card_counter'],
                            os.path.basename(source_path)
                        )
                        final_dest_root = os.path.join(dest_root, resolved_path)
                    elif self.job_params['create_source_folder']:
                        final_dest_root = os.path.join(dest_root, os.path.basename(source_path))
                    else: final_dest_root = dest_root
                    
                    relative_path = os.path.relpath(source_file, source_path)
                    resolved_dests[source_file].append(os.path.join(final_dest_root, relative_path))

        self.job_params['total_size'] = total_size
        self.job_params['resolved_dests'] = resolved_dests
        self.scan_finished.emit(self.job_params)

class EjectWorker(QThread):
    # This worker is also well-structured and does not need changes.
    ejection_finished = Signal(str, bool)
    def __init__(self, path_to_eject, parent=None):
        super().__init__(parent)
        self.mount_path = path_to_eject
    def run(self):
        try:
            device_path = next((p.device for p in psutil.disk_partitions() if p.mountpoint == self.mount_path), None)
            if not device_path:
                self.ejection_finished.emit(self.mount_path, False); return

            system = platform.system()
            if system == "Darwin":
                try: subprocess.run(["diskutil", "eject", device_path], check=True, capture_output=True)
                except subprocess.CalledProcessError: subprocess.run(["diskutil", "unmountDisk", "force", device_path], check=True, capture_output=True)
            elif system == "Linux": subprocess.run(["eject", device_path], check=True, capture_output=True)
            elif system == "Windows":
                drive_letter = os.path.splitdrive(self.mount_path)[0]
                command = ["powershell", f"(New-Object -comObject Shell.Application).Namespace(17).ParseName('{drive_letter}').InvokeVerb('Eject')"]
                subprocess.run(command, check=True, capture_output=True)
            else:
                self.ejection_finished.emit(self.mount_path, False); return
            self.ejection_finished.emit(self.mount_path, True)
        except Exception: self.ejection_finished.emit(self.mount_path, False)

class PostProcessWorker(QThread):
    # This worker is fine, but we'll add some annotations for consistency.
    progress = Signal(int, int, str)
    file_processed = Signal(str, str, dict)
    job_processed = Signal(str)
    def __init__(self, job: Job, project_path, parent=None):
        super().__init__(parent)
        self.job = job
        self.project_path = project_path
        self._ffprobe_available = check_command(FFPROBE_PATH)
        self._ffmpeg_available = check_command(FFMPEG_PATH)

    def run(self):
        files_to_process = self.job.report.get('files', [])
        total_files = len(files_to_process)
        for i, file_info in enumerate(files_to_process):
            self.progress.emit(i + 1, total_files, os.path.basename(file_info['source']))
            updates = {}
            verified_dest = next((d['path'] for d in file_info['destinations'] if d.get('verified')), None)
            if self._is_video_file(file_info['source']) and verified_dest:
                if self._ffprobe_available:
                    updates['metadata'] = self._get_video_metadata(verified_dest)
                updates['thumbnail'] = self._create_thumbnail_robust(verified_dest)
            if updates:
                self.file_processed.emit(self.job.id, file_info['source'], updates)
        self.job_processed.emit(self.job.id)

    def _is_video_file(self, file_path: str) -> bool:
        return any(file_path.lower().endswith(ext) for ext in ['.mov', '.mp4', '.mxf', '.avi', '.r3d', '.braw'])

    def _get_video_metadata(self, file_path: str) -> dict:
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
                    "fps": eval(video_stream.get('r_frame_rate', '0/1')),
                    "duration": f"{float(video_stream.get('duration', 0)):.2f}s"}
        except Exception: return {}
        
    def _create_thumbnail_robust(self, video_path: str, thumb_size: tuple = (160, 90)) -> str | None:
        thumb_path = self._get_thumb_path(video_path)
        if os.path.exists(thumb_path): return thumb_path
        
        try: # First, try with OpenCV as it's often faster
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

        if self._ffmpeg_available: # Fallback to FFmpeg for broader codec support
            try:
                creationflags = subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0
                cmd = [ FFMPEG_PATH, '-y', '-i', video_path, '-vf', f'scale={thumb_size[0]}:{thumb_size[1]}:force_original_aspect_ratio=decrease,pad={thumb_size[0]}:{thumb_size[1]}:(ow-iw)/2:(oh-ih)/2', '-ss', '00:00:01', '-vframes', '1', thumb_path ]
                subprocess.run(cmd, check=True, capture_output=True, creationflags=creationflags)
                return thumb_path if os.path.exists(thumb_path) else None
            except Exception: return None
        return None

    def _get_thumb_path(self, video_path: str) -> str:
        temp_dir = os.path.join(self.project_path, ".dit_project", "thumbnails")
        os.makedirs(temp_dir, exist_ok=True)
        thumb_name = f"{os.path.basename(video_path)}_{xxhash.xxh64(video_path.encode()).hexdigest()}.jpg"
        return os.path.join(temp_dir, thumb_name)

class TransferWorker(QThread):
    # --- MODIFICATION: The run() method is now a high-level coordinator ---
    progress = Signal(str, int, float, int)
    file_progress = Signal(int, str, str, float)
    job_finished = Signal(Job)
    error = Signal(str)

    def __init__(self, job: Job, project_path, parent=None):
        super().__init__(parent)
        self.job = job
        self.project_path = project_path
        self.is_paused = False
        self.is_cancelled = False
        self.CHUNK_SIZE = 4 * 1024 * 1024

    def run(self):
        """
        Main worker loop. Orchestrates the transfer process by calling helper methods.
        This high-level view makes the overall logic clear.
        """
        report = self._initialize_report()
        try:
            files_to_process = self._collect_files_and_size(report)
            self._process_all_files(files_to_process, report)
            self._handle_ejection(report)
        except Exception as e:
            self.job.status = JobStatus.COMPLETED_WITH_ERRORS
            report['errors'].append(f"Critical error: {e}")
            self.error.emit(f"A critical error occurred: {e}")
        
        self._finalize_job(report)

    def _initialize_report(self) -> dict:
        """Creates the initial report dictionary for this job."""
        return {
            'job_id': self.job.id, 'start_time': datetime.now(), 'sources': self.job.sources,
            'destinations': self.job.destinations, 'checksum_method': self.job.checksum_method,
            'files': [], 'total_size': 0, 'errors': []
        }

    def _collect_files_and_size(self, report: dict) -> list[str]:
        """Scans source paths to gather a list of files and calculate total size."""
        self.progress.emit(self.job.id, 0, 0.0, -1)
        files_to_process = []
        for source_file_path in self.job.resolved_dests.keys():
            try:
                size = os.path.getsize(source_file_path)
                files_to_process.append(source_file_path)
                report['total_size'] += size
            except FileNotFoundError:
                continue
        return files_to_process
    
    def _process_all_files(self, files_to_process: list[str], report: dict):
        """Iterates through all files, handling cancellation and progress updates."""
        total_bytes_processed = 0
        job_start_time = time.monotonic()

        for source_file_path in files_to_process:
            if self._check_for_pause_or_cancel():
                self.job.status = JobStatus.CANCELLED
                return
            
            try:
                file_info = self._process_single_file(source_file_path, report)
                report['files'].append(file_info)
                if file_info['status'] == 'Verified':
                    total_bytes_processed += file_info.get('size', 0)
            except Exception as e:
                error_str = f"Error processing {os.path.basename(source_file_path)}: {e}"
                report['errors'].append(error_str)
                self.error.emit(error_str)
            
            self._update_overall_progress(total_bytes_processed, report['total_size'], job_start_time)
            
    def _process_single_file(self, source_path: str, report: dict) -> dict:
        """Handles the copy, hash, and verification logic for one file."""
        base_source_path = next((sp for sp in self.job.sources if source_path.startswith(sp)), self.job.sources[0])
        file_info = {
            'source': source_path, 'destinations': [], 'status': 'Failed', 'checksum': '',
            'custom_metadata': self.job.metadata.get(base_source_path, {})
        }

        source_hash = None
        if self.job.verification_mode == "full":
            self.file_progress.emit(0, f"Hashing {os.path.basename(source_path)}...", source_path, 0.0)
            source_hash = self._calculate_hash(source_path, self.job.checksum_method)
            file_info['checksum'] = source_hash
        
        file_info['size'] = os.path.getsize(source_path)
        
        all_dests_verified = True
        for dest_path in self.job.resolved_dests[source_path]:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            should_skip = (self.job.skip_existing and os.path.exists(dest_path) and
                           file_info['size'] == os.path.getsize(dest_path))
            
            if not should_skip:
                self._copy_file_with_progress(source_path, dest_path)

            if not self._verify_destination(dest_path, file_info, source_hash, report):
                all_dests_verified = False

        if all_dests_verified:
            file_info['status'] = 'Verified'
        elif self.job.verification_mode == "none":
            file_info['status'] = 'Copied (Unverified)'
        
        return file_info
    
    def _verify_destination(self, dest_path: str, file_info: dict, source_hash: str | None, report: dict) -> bool:
        """Verifies a single destination file and updates the file_info."""
        dest_info = {'path': dest_path, 'verified': False}
        file_info['destinations'].append(dest_info)

        if self.job.verification_mode == "none":
            dest_info['status'] = 'Unverified'
            return True

        if not os.path.exists(dest_path):
            dest_info['status'] = "Missing"
            report['errors'].append(f"{os.path.basename(file_info['source'])}: Missing at dest")
            return False

        if file_info['size'] != os.path.getsize(dest_path):
            dest_info['status'] = "Size Mismatch"
            report['errors'].append(f"{os.path.basename(file_info['source'])}: Size Mismatch")
            return False

        if self.job.verification_mode == "size":
            dest_info['verified'] = True
            dest_info['status'] = 'Verified (Size Only)'
            return True

        self.file_progress.emit(50, f"Verifying...", dest_path, 0.0)
        dest_hash = self._calculate_hash(dest_path, self.job.checksum_method)
        if source_hash == dest_hash:
            dest_info['verified'] = True
            dest_info['status'] = 'Verified'
            return True
        else:
            dest_info['status'] = "Verification FAILED"
            report['errors'].append(f"{os.path.basename(file_info['source'])}: Verification failed")
            return False

    def _update_overall_progress(self, processed_bytes: int, total_bytes: int, start_time: float):
        """Calculates and emits the overall progress signal for the job."""
        elapsed = time.monotonic() - start_time
        speed = (processed_bytes / (1024*1024)) / elapsed if elapsed > 0 else 0
        eta = (total_bytes - processed_bytes) / (speed * 1024 * 1024) if speed > 0 else -1
        self.progress.emit(self.job.id, processed_bytes, speed, eta)

    def _handle_ejection(self, report: dict):
        """Determines which sources can be ejected and adds them to the report."""
        if self.job.eject_on_completion:
            ejectable_sources = []
            unique_sources = set(self.job.sources)
            for source_path in unique_sources:
                if not os.path.ismount(source_path): continue
                is_source_fully_verified = all(
                    f['status'] == 'Verified' for f in report['files'] 
                    if f['source'].startswith(source_path)
                )
                if is_source_fully_verified:
                    ejectable_sources.append(source_path)
            report['ejectable_sources_on_success'] = ejectable_sources
    
    def _finalize_job(self, report: dict):
        """Sets the final job status based on the report and emits the finished signal."""
        if self.job.status == JobStatus.CANCELLED:
            # Status already set by cancellation logic, just emit.
            pass
        elif report['errors']:
            self.job.status = JobStatus.COMPLETED_WITH_ERRORS
        else:
            self.job.status = JobStatus.COMPLETED
        
        report['status'] = self.job.status.name
        report['end_time'] = datetime.now()
        self.job.report = report
        self.job_finished.emit(self.job)

    def _copy_file_with_progress(self, src, dst):
        # ... (unchanged)
        src_size = os.path.getsize(src)
        copied_bytes = 0
        file_mode = 'wb'; start_pos = 0
        if self.job.resume_partial and os.path.exists(dst):
            dest_size = os.path.getsize(dst)
            if 0 < dest_size < src_size:
                file_mode = 'ab'; start_pos = dest_size; copied_bytes = dest_size
        with open(src, 'rb') as fsrc, open(dst, file_mode) as fdst:
            if start_pos > 0: fsrc.seek(start_pos)
            while True:
                if self._check_for_pause_or_cancel(): raise InterruptedError("Copy cancelled by user")
                buf = fsrc.read(self.CHUNK_SIZE)
                if not buf: break
                fdst.write(buf)
                copied_bytes += len(buf)
                percent = int((copied_bytes / src_size) * 100) if src_size > 0 else 100
                self.file_progress.emit(percent, "Copying...", src, 0)
        shutil.copystat(src, dst)
        
    def _calculate_hash(self, file_path: str, method: str) -> str:
        hasher = xxhash.xxh64() if method == "xxHash (Fast)" else hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(self.CHUNK_SIZE): hasher.update(chunk)
        return hasher.hexdigest()

    def _check_for_pause_or_cancel(self) -> bool:
        """Helper to pause execution or check for cancellation. Returns True if cancelled."""
        while self.is_paused:
            time.sleep(0.5)
            if self.is_cancelled:
                return True
        return self.is_cancelled

    def pause(self): self.is_paused = True
    def resume(self): self.is_paused = False
    def cancel(self): self.is_cancelled = True; self.is_paused = False

class MHLVerifyWorker(QThread):
    # This worker is well-structured, but we'll refactor its run method for consistency.
    progress = Signal(int, str, float, int)
    file_progress = Signal(int, str, str, float)
    job_finished = Signal(Job)
    error = Signal(str)

    def __init__(self, job: Job, parent=None):
        super().__init__(parent)
        self.job = job
        self.is_paused = False
        self.is_cancelled = False
        self.CHUNK_SIZE = 8 * 1024 * 1024

    def run(self):
        report = self._initialize_report()
        try:
            files_to_verify = self._parse_mhl(self.job.mhl_file)
            self._verify_all_files(files_to_verify, report)
        except Exception as e:
            report['errors'].append(f"A critical error occurred: {e}")
            self.error.emit(str(e))
        
        self._finalize_job(report)

    def _initialize_report(self) -> dict:
        return {
            'job_id': self.job.id, 'start_time': datetime.now(),
            'mhl_file': self.job.mhl_file, 'target_dir': self.job.target_dir,
            'files': [], 'errors': [], 'verified_count': 0, 'failed_count': 0, 'missing_count': 0
        }
    
    def _verify_all_files(self, files_to_verify: list, report: dict):
        total_files = len(files_to_verify)
        for i, file_data in enumerate(files_to_verify):
            if self._check_for_pause_or_cancel():
                self.job.status = JobStatus.CANCELLED
                return

            relative_path, expected_hash, hash_type, _ = file_data
            full_path = os.path.join(self.job.target_dir, relative_path)
            file_report = {'path': full_path, 'expected_hash': expected_hash, 'hash_type': hash_type}

            if not os.path.exists(full_path):
                file_report['status'] = 'Missing'
                report['missing_count'] += 1
            else:
                self.file_progress.emit(0, f"Verifying {os.path.basename(full_path)}...", full_path, 0.0)
                actual_hash = self._calculate_hash(full_path, hash_type)
                if actual_hash == expected_hash:
                    file_report['status'] = 'Verified'
                    report['verified_count'] += 1
                else:
                    file_report['status'] = 'FAILED'
                    report['failed_count'] += 1
                    file_report['actual_hash'] = actual_hash
            
            report['files'].append(file_report)
            progress_percent = int(((i + 1) / total_files) * 100) if total_files > 0 else 100
            self.progress.emit(progress_percent, f"Verified file {i + 1} of {total_files}", 0.0, -1)
    
    def _finalize_job(self, report: dict):
        if self.job.status == JobStatus.CANCELLED:
            pass
        elif report['failed_count'] > 0 or report['missing_count'] > 0:
            self.job.status = JobStatus.COMPLETED_WITH_ERRORS
        else:
            self.job.status = JobStatus.COMPLETED
        
        report['status'] = self.job.status.name
        report['end_time'] = datetime.now()
        self.job.report = report
        self.job_finished.emit(self.job)

    def _parse_mhl(self, file_path):
        # ... (unchanged)
        files = []
        tree = ET.parse(file_path)
        root = tree.getroot()
        ns = {'mhl': 'http://www.movielabs.com/ACF/MHL/v1.0'}
        hash_elements = root.findall('.//mhl:hash', ns)
        if not hash_elements: hash_elements = root.findall('.//hash')
        for hash_elem in hash_elements:
            file_path_elem = hash_elem.find('mhl:file', ns) or hash_elem.find('file')
            size_elem = hash_elem.find('mhl:size', ns) or hash_elem.find('size')
            hash_val, hash_type = None, None
            if (elem := hash_elem.find('mhl:xxhash64', ns) or hash_elem.find('xxhash64')) is not None:
                hash_val, hash_type = elem.text, 'xxhash64'
            elif (elem := hash_elem.find('mhl:md5', ns) or hash_elem.find('md5')) is not None:
                hash_val, hash_type = elem.text, 'md5'
            if file_path_elem is not None and hash_val:
                files.append((file_path_elem.text, hash_val, hash_type, int(size_elem.text)))
        return files

    def _calculate_hash(self, file_path, method):
        # ... (unchanged)
        hasher = xxhash.xxh64() if method == "xxhash64" else hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(self.CHUNK_SIZE): hasher.update(chunk)
        return hasher.hexdigest()

    def _check_for_pause_or_cancel(self) -> bool:
        while self.is_paused:
            time.sleep(0.5)
            if self.is_cancelled:
                return True
        return self.is_cancelled

    def pause(self): self.is_paused = True
    def resume(self): self.is_paused = False
    def cancel(self): self.is_cancelled = True; self.is_paused = False

class ReportWorker(QThread):
    # This worker is already well-structured and does not need changes.
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