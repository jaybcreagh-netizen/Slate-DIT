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
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from utils import format_bytes, check_command, resolve_path_template 

class ScanWorker(QThread):
    """
    Scans source directories in the background to avoid freezing the UI.
    Calculates total size, file list, and resolved destination paths.
    """
    scan_finished = Signal(dict)

    def __init__(self, job_params, parent=None):
        super().__init__(parent)
        self.job_params = job_params

    def run(self):
        sources = self.job_params['sources']
        destinations = self.job_params['destinations']
        
        all_source_files = {}
        total_size = 0
        resolved_dests = {}

        for source_path in sources:
            source_files_in_dir = []
            for root, _, files in os.walk(source_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    all_source_files[full_path] = source_path
                    source_files_in_dir.append(full_path)
                    try:
                        total_size += os.path.getsize(full_path)
                    except FileNotFoundError:
                        continue
            
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
                    else:
                        final_dest_root = dest_root
                    
                    relative_path = os.path.relpath(source_file, source_path)
                    resolved_dests[source_file].append(os.path.join(final_dest_root, relative_path))

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
        files_to_process = self.job['report']['files']
        total_files = len(files_to_process)
        job_id = self.job['id']

        for i, file_info in enumerate(files_to_process):
            self.progress.emit(i + 1, total_files, os.path.basename(file_info['source']))
            updates = {}
            verified_dest = next((d['path'] for d in file_info['destinations'] if d.get('verified')), None)

            if self._is_video_file(file_info['source']) and verified_dest:
                if self._ffprobe_available:
                    updates['metadata'] = self._get_video_metadata(verified_dest)
                updates['thumbnail'] = self._create_thumbnail_robust(verified_dest)
            
            if updates:
                self.file_processed.emit(job_id, file_info['source'], updates)
        
        self.job_processed.emit(job_id)

    def _is_video_file(self, file_path):
        return any(file_path.lower().endswith(ext) for ext in ['.mov', '.mp4', '.mxf', '.avi', '.r3d', '.braw'])

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
                    "fps": eval(video_stream.get('r_frame_rate', '0/1')),
                    "duration": f"{float(video_stream.get('duration', 0)):.2f}s"}
        except Exception: return {}
        
    def _create_thumbnail_robust(self, video_path, thumb_size=(160, 90)):
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

    def _get_thumb_path(self, video_path):
        temp_dir = os.path.join(self.project_path, ".dit_project", "thumbnails")
        os.makedirs(temp_dir, exist_ok=True)
        thumb_name = f"{os.path.basename(video_path)}_{xxhash.xxh64(video_path.encode()).hexdigest()}.jpg"
        return os.path.join(temp_dir, thumb_name)

class TransferWorker(QThread):
    # --- MODIFIED SIGNAL: Emits bytes processed, not percentage ---
    progress = Signal(str, int, float, int) # job_id, bytes_processed, speed, eta
    file_progress = Signal(int, str, str, float)
    job_finished = Signal(dict); error = Signal(str)

    def __init__(self, job, project_path, parent=None):
        super().__init__(parent)
        self.job = job; self.project_path = project_path
        self.is_paused = False; self.is_cancelled = False
        self.CHUNK_SIZE = 4 * 1024 * 1024

    def run(self):
        source_paths=self.job['sources']
        checksum_method=self.job['checksum_method']
        job_type = self.job.get("job_type", "copy")
        verification_mode = self.job.get("verification_mode", "full")
        
        report_data = {'job_id': self.job['id'], 'start_time': datetime.now(), 'sources': source_paths,
                       'destinations': self.job['destinations'], 'checksum_method': checksum_method, 'files': [],
                       'status': 'Completed', 'total_size': 0, 'errors': []}
        try:
            self.progress.emit(self.job['id'], 0, 0.0, -1)
            all_source_files = {}
            for source_path in source_paths:
                for root, _, files in os.walk(source_path):
                    for file in files:
                        if self.is_cancelled: report_data['status']='Cancelled'; self.job_finished.emit(report_data); return
                        full_path = os.path.join(root, file); all_source_files[full_path] = source_path
            
            files_to_process = []
            for source_file_path, base_source_path in all_source_files.items():
                try:
                    size = os.path.getsize(source_file_path)
                    files_to_process.append((source_file_path, base_source_path)); report_data['total_size'] += size
                except FileNotFoundError: continue

            self.progress.emit(self.job['id'], 0, 0.0, -1)

            total_bytes_processed = 0
            job_start_time = time.monotonic()

            for i, (source_file_path, base_source_path) in enumerate(files_to_process):
                while self.is_paused:
                    if self.is_cancelled: break
                    time.sleep(0.5)
                if self.is_cancelled: report_data['status']='Cancelled'; self.job_finished.emit(report_data); return
                
                file_info = {'source': source_file_path, 'destinations': [], 'status': 'Failed', 'checksum': '',
                             'custom_metadata': self.job['metadata'].get(base_source_path, {})}
                
                try:
                    source_hash = None
                    if verification_mode == "full":
                        self.file_progress.emit(0, f"Hashing {os.path.basename(source_file_path)}...", source_file_path, 0.0)
                        source_hash = self._calculate_hash(source_file_path, checksum_method)
                        file_info['checksum'] = source_hash
                    
                    file_info['size'] = os.path.getsize(source_file_path)
                    
                    verified_all_dests = True
                    for dest_file_path in self.job['resolved_dests'][source_file_path]:
                        os.makedirs(os.path.dirname(dest_file_path), exist_ok=True)
                        
                        should_skip = False
                        if self.job.get('skip_existing', False) and os.path.exists(dest_file_path):
                            if file_info['size'] == os.path.getsize(dest_file_path):
                                should_skip = True
                        if not should_skip:
                            self._copy_file_with_progress(source_file_path, dest_file_path)

                        if verification_mode == "none":
                            file_info['destinations'].append({'path': dest_file_path, 'verified': False, 'status': 'Unverified'})
                            continue
                        
                        dest_info = {'path': dest_file_path, 'verified': False}

                        if not os.path.exists(dest_file_path):
                            verified_all_dests = False
                            dest_info['status'] = "Missing"
                            report_data['errors'].append(f"{os.path.basename(source_file_path)}: Missing")
                            file_info['destinations'].append(dest_info)
                            continue

                        if file_info['size'] != os.path.getsize(dest_file_path):
                            verified_all_dests = False
                            dest_info['status'] = "Size Mismatch"
                            report_data['errors'].append(f"{os.path.basename(source_file_path)}: Size Mismatch")
                            file_info['destinations'].append(dest_info)
                            continue

                        if verification_mode == "size":
                            dest_info['verified'] = True
                            dest_info['status'] = 'Verified (Size Only)'
                            file_info['destinations'].append(dest_info)
                            continue

                        self.file_progress.emit(50, f"Verifying...", dest_file_path, 0.0)
                        dest_hash = self._calculate_hash(dest_file_path, checksum_method)
                        if source_hash == dest_hash:
                            dest_info['verified'] = True
                        else:
                            verified_all_dests = False
                            dest_info['status'] = "Verification FAILED"
                            report_data['errors'].append(f"{os.path.basename(source_file_path)}: Verification failed")
                        file_info['destinations'].append(dest_info)

                    if verified_all_dests:
                        file_info['status'] = 'Verified'
                    elif verification_mode == "none":
                        file_info['status'] = 'Copied (Unverified)'
                    
                    report_data['files'].append(file_info)

                except Exception as e:
                    error_str = f"Error processing {os.path.basename(source_file_path)}: {e}"
                    file_info['status'] = f'Error: {e}'; report_data['files'].append(file_info)
                    report_data['errors'].append(error_str)
                    self.error.emit(error_str)
                
                if file_info['status'] == 'Verified':
                    total_bytes_processed += file_info.get('size', 0)
                
                elapsed_time = time.monotonic() - job_start_time
                speed_mbps = (total_bytes_processed / (1024*1024)) / elapsed_time if elapsed_time > 0 else 0
                
                bytes_remaining = report_data['total_size'] - total_bytes_processed
                eta_seconds = bytes_remaining / (speed_mbps * 1024 * 1024) if speed_mbps > 0 else -1

                self.progress.emit(self.job['id'], total_bytes_processed, speed_mbps, eta_seconds)
            
            report_data['ejectable_sources_on_success'] = []
            if self.job.get('eject_on_completion', False):
                ejectable_sources = []
                unique_sources = set(self.job['sources'])
                for source_path in unique_sources:
                    if not os.path.ismount(source_path): continue
                    is_source_fully_verified = all(
                        f['status'] == 'Verified' for f in report_data['files'] 
                        if f['source'].startswith(source_path)
                    )
                    if is_source_fully_verified:
                        ejectable_sources.append(source_path)
                report_data['ejectable_sources_on_success'] = ejectable_sources

        except Exception as e:
            report_data['status'] = 'Failed'; self.error.emit(f"A critical error occurred: {e}")
            report_data['errors'].append(f"Critical error: {e}")
        
        if report_data['errors']:
            report_data['status'] = 'Completed with errors'

        report_data['end_time'] = datetime.now(); self.job_finished.emit(report_data)

    def _copy_file_with_progress(self, src, dst):
        src_size = os.path.getsize(src)
        copied_bytes = 0
        file_mode = 'wb'
        start_pos = 0

        if self.job.get('resume_partial', False) and os.path.exists(dst):
            dest_size = os.path.getsize(dst)
            if 0 < dest_size < src_size:
                file_mode = 'ab'
                start_pos = dest_size
                copied_bytes = dest_size
        
        with open(src, 'rb') as fsrc, open(dst, file_mode) as fdst:
            if start_pos > 0: fsrc.seek(start_pos)
            while True:
                while self.is_paused: time.sleep(0.5)
                if self.is_cancelled: raise InterruptedError("Copy cancelled by user")
                
                buf = fsrc.read(self.CHUNK_SIZE)
                if not buf: break
                fdst.write(buf)
                
                copied_bytes += len(buf)
                percent = int((copied_bytes / src_size) * 100) if src_size > 0 else 100
                self.file_progress.emit(percent, "Copying...", src, 0)
        
        shutil.copystat(src, dst)
        
    def _calculate_hash(self, file_path, method):
        hasher = xxhash.xxh64() if method == "xxHash (Fast)" else hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(self.CHUNK_SIZE): hasher.update(chunk)
        return hasher.hexdigest()

    def pause(self): self.is_paused = True
    def resume(self): self.is_paused = False
    def cancel(self): self.is_cancelled = True; self.is_paused = False

class MHLVerifyWorker(QThread):
    progress = Signal(int, str, float, int)
    file_progress = Signal(int, str, str, float)
    job_finished = Signal(dict)
    error = Signal(str)

    def __init__(self, job, parent=None):
        super().__init__(parent)
        self.job = job
        self.is_paused = False
        self.is_cancelled = False
        self.CHUNK_SIZE = 8 * 1024 * 1024

    def run(self):
        mhl_file_path = self.job['mhl_file']
        target_dir = self.job['target_dir']

        report_data = {'job_id': self.job['id'], 'start_time': datetime.now(),
                       'mhl_file': mhl_file_path, 'target_dir': target_dir,
                       'files': [], 'status': 'Completed', 'errors': [],
                       'verified_count': 0, 'failed_count': 0, 'missing_count': 0}
        
        try:
            self.progress.emit(-1, "Parsing MHL file...", 0.0, -1)
            files_to_verify = self._parse_mhl(mhl_file_path)
            total_files = len(files_to_verify)
            
            job_start_time = time.monotonic()
            
            for i, file_info in enumerate(files_to_verify):
                while self.is_paused:
                    if self.is_cancelled: break
                    time.sleep(0.5)
                if self.is_cancelled:
                    report_data['status'] = 'Cancelled'
                    self.job_finished.emit(report_data)
                    return

                relative_path, expected_hash, hash_type, size = file_info
                full_path = os.path.join(target_dir, relative_path)
                
                file_report = {'path': full_path, 'expected_hash': expected_hash, 'hash_type': hash_type}

                if not os.path.exists(full_path):
                    file_report['status'] = 'Missing'
                    report_data['missing_count'] += 1
                else:
                    self.file_progress.emit(0, f"Verifying {os.path.basename(full_path)}...", full_path, 0.0)
                    actual_hash = self._calculate_hash(full_path, hash_type)
                    if actual_hash == expected_hash:
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

        except Exception as e:
            error_msg = f"A critical error occurred: {e}"
            report_data['status'] = 'Failed'
            report_data['errors'].append(error_msg)
            self.error.emit(error_msg)

        if report_data['failed_count'] > 0 or report_data['missing_count'] > 0:
            report_data['status'] = 'Completed with issues'
        
        report_data['end_time'] = datetime.now()
        self.job_finished.emit(report_data)

    def _parse_mhl(self, file_path):
        files = []
        tree = ET.parse(file_path)
        root = tree.getroot()
        ns = {'mhl': 'http://www.movielabs.com/ACF/MHL/v1.0'}
        hash_elements = root.findall('.//mhl:hash', ns)
        if not hash_elements:
            hash_elements = root.findall('.//hash')

        for hash_elem in hash_elements:
            file_path_elem = hash_elem.find('mhl:file', ns) or hash_elem.find('file')
            size_elem = hash_elem.find('mhl:size', ns) or hash_elem.find('size')
            
            hash_val, hash_type = None, None
            xxhash64_elem = hash_elem.find('mhl:xxhash64', ns) or hash_elem.find('xxhash64')
            md5_elem = hash_elem.find('mhl:md5', ns) or hash_elem.find('md5')

            if xxhash64_elem is not None:
                hash_val, hash_type = xxhash64_elem.text, 'xxhash64'
            elif md5_elem is not None:
                hash_val, hash_type = md5_elem.text, 'md5'
            
            if file_path_elem is not None and hash_val:
                files.append((file_path_elem.text, hash_val, hash_type, int(size_elem.text)))
        return files

    def _calculate_hash(self, file_path, method):
        hasher = xxhash.xxh64() if method == "xxhash64" else hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(self.CHUNK_SIZE):
                hasher.update(chunk)
        return hasher.hexdigest()

    def pause(self): self.is_paused = True
    def resume(self): self.is_paused = False
    def cancel(self): self.is_cancelled = True; self.is_paused = False

    # ... (at the end of workers.py, after MHLVerifyWorker)

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from utils import format_bytes

class ReportWorker(QThread):
    """ Generates a PDF report in a background thread to avoid freezing the UI. """
    finished = Signal(bool, str, str)  # success, file_path, error_message

    def __init__(self, report_generator_func, report_data, file_path, parent=None):
        super().__init__(parent)
        self.report_generator_func = report_generator_func
        self.report_data = report_data
        self.file_path = file_path

    def run(self):
        try:
            # The report_generator_func is the method from ReportManager that contains
            # all the reportlab logic. This worker just executes it off the main thread.
            self.report_generator_func(self.report_data, self.file_path)
            self.finished.emit(True, self.file_path, "")
        except Exception as e:
            self.finished.emit(False, self.file_path, str(e))