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


from PIL import Image as PILImage
import rawpy

from config import FFMPEG_PATH, FFPROBE_PATH
from utils import check_command, resolve_path_template

# --- ScanWorker, EjectWorker, PostProcessWorker are unchanged ---
class ScanWorker(QThread):
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
            for root, _, files in os.walk(source_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    all_source_files[full_path] = source_path
                    try:
                        total_size += os.path.getsize(full_path)
                    except FileNotFoundError:
                        continue
                    resolved_dests[full_path] = []
                    relative_path = os.path.relpath(full_path, source_path)
                    for dest_root in destinations:
                        final_dest_root = dest_root
                        if self.job_params['has_template']:
                            source_folder_name = os.path.basename(source_path.rstrip(os.path.sep))
                            resolved_template_path = resolve_path_template(
                                self.job_params['naming_preset']['template'],
                                self.job_params['naming_preset'],
                                self.job_params['card_counter'],
                                source_folder_name
                            )
                            final_dest_root = os.path.join(dest_root, resolved_template_path)
                        elif self.job_params['create_source_folder']:
                            source_folder_name = os.path.basename(source_path.rstrip(os.path.sep))
                            final_dest_root = os.path.join(dest_root, source_folder_name)
                        resolved_dests[full_path].append(os.path.join(final_dest_root, relative_path))
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
                    "fps": eval(video_stream.get('r_frame_rate', '0/1')),
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

# --- START REFACTOR: Simultaneous Hashing Implementation ---
class TransferWorker(QThread):
    progress = Signal(str, float, float, int)
    file_progress = Signal(int, str, str, float)
    job_finished = Signal(dict)
    error = Signal(str, str)
    def __init__(self, job, project_path, parent=None):
        super().__init__(parent)
        self.job = job; self.project_path = project_path
        self.is_paused = False; self.is_cancelled = False
        self.CHUNK_SIZE = 4 * 1024 * 1024
        
    def run(self):
        checksum_method = self.job['checksum_method']
        verification_mode = self.job.get("verification_mode", "full")
        report_data = {
            'job_id': self.job['id'], 'start_time': datetime.now(), 
            'sources': self.job['sources'], 'destinations': self.job['destinations'],
            'checksum_method': checksum_method, 'files': [],
            'status': 'Completed', 'total_size': self.job['report']['total_size'], 'errors': []
        }
        
        total_bytes_processed_in_job = 0
        job_start_time = time.monotonic()

        for source_file_path in self.job['resolved_dests']:
            # --- Main control loop for each file ---
            while self.is_paused:
                if self.is_cancelled: break
                time.sleep(0.5)
            if self.is_cancelled:
                report_data['status'] = 'Cancelled'
                self.job_finished.emit(report_data)
                return

            base_source_path = next((sp for sp in self.job['sources'] if source_file_path.startswith(sp)), None)
            file_info = {
                'source': source_file_path, 'destinations': [], 'status': 'Failed', 
                'checksum': '', 'custom_metadata': self.job['metadata'].get(base_source_path, {})
            }

            try:
                file_size = os.path.getsize(source_file_path)
                file_info['size'] = file_size

                # --- Core Logic Change ---
                source_hash = None
                hasher = None
                if verification_mode == "full":
                    self.file_progress.emit(0, "Copying & Hashing...", source_file_path, 0.0)
                    hasher = xxhash.xxh64() if checksum_method == "xxHash (Fast)" else hashlib.md5()
                    
                    # This single call now does the copy and calculates the source hash simultaneously
                    self._copy_and_hash_file(source_file_path, self.job['resolved_dests'][source_file_path], hasher)
                    source_hash = hasher.hexdigest()
                    file_info['checksum'] = source_hash
                else:
                    # If not verifying by hash, just do a simple copy.
                    self.file_progress.emit(0, "Copying...", source_file_path, 0.0)
                    for dest_path in self.job['resolved_dests'][source_file_path]:
                        self._copy_and_hash_file(source_file_path, [dest_path], None) # Pass None for hasher
                
                # --- Verification Logic (Largely Unchanged) ---
                verified_all_dests = True
                for dest_path in self.job['resolved_dests'][source_file_path]:
                    dest_info = {'path': dest_path, 'verified': False}
                    if not os.path.exists(dest_path) or file_size != os.path.getsize(dest_path):
                        verified_all_dests = False
                        dest_info['status'] = "Size Mismatch or Missing"
                    elif verification_mode == "none":
                        dest_info['status'] = "Unverified"
                    elif verification_mode == "size":
                        dest_info['verified'] = True
                        dest_info['status'] = "Verified (Size Only)"
                    elif verification_mode == "full":
                        self.file_progress.emit(50, "Verifying...", dest_path, 0.0)
                        dest_hash = self._calculate_hash(dest_path, checksum_method)
                        if source_hash == dest_hash:
                            dest_info['verified'] = True
                        else:
                            verified_all_dests = False
                            dest_info['status'] = "Verification FAILED"
                    file_info['destinations'].append(dest_info)

                if verified_all_dests:
                    file_info['status'] = 'Verified'
                report_data['files'].append(file_info)

            except Exception as e:
                error_str = f"Error processing {os.path.basename(source_file_path)}: {e}"
                file_info['status'] = f'Error: {e}'; report_data['files'].append(file_info)
                report_data['errors'].append(error_str)
                self.error.emit(error_str, self.job['id'])

            total_bytes_processed_in_job += file_info.get('size', 0)
            elapsed_time = time.monotonic() - job_start_time
            speed_mbps = (total_bytes_processed_in_job / (1024*1024)) / elapsed_time if elapsed_time > 0 else 0
            bytes_remaining = report_data['total_size'] - total_bytes_processed_in_job
            eta_seconds = bytes_remaining / (speed_mbps * 1024 * 1024) if speed_mbps > 0 else -1
            self.progress.emit(self.job['id'], total_bytes_processed_in_job, speed_mbps, int(eta_seconds))

        if report_data['errors']:
            report_data['status'] = 'Completed with errors'
        report_data['end_time'] = datetime.now()
        self.job_finished.emit(report_data)

    def _copy_and_hash_file(self, src_path, dest_paths, hasher):
        """
        Reads the source file once, updating the hasher and writing to all
        destination files simultaneously.
        """
        src_size = os.path.getsize(src_path)
        copied_bytes = 0
        
        # Open all file handles at once
        dest_files = []
        try:
            for dest_path in dest_paths:
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                dest_files.append(open(dest_path, 'wb'))
            
            with open(src_path, 'rb') as fsrc:
                while True:
                    while self.is_paused: time.sleep(0.5)
                    if self.is_cancelled: raise InterruptedError("Copy cancelled by user")
                    
                    buf = fsrc.read(self.CHUNK_SIZE)
                    if not buf:
                        break
                    
                    # Update hash if a hasher object was provided
                    if hasher:
                        hasher.update(buf)
                    
                    # Write the same buffer to all open destination files
                    for fdst in dest_files:
                        fdst.write(buf)

                    copied_bytes += len(buf)
                    percent = int((copied_bytes / src_size) * 100) if src_size > 0 else 100
                    self.file_progress.emit(percent, "Copying...", src_path, 0)
        
        finally:
            # Ensure all destination files are closed
            for fdst in dest_files:
                fdst.close()

        # Copy file metadata (timestamps, etc.) after closing
        for dest_path in dest_paths:
            if os.path.exists(dest_path):
                shutil.copystat(src_path, dest_path)

    def _calculate_hash(self, file_path, method):
        """
        Calculates the hash of a file. Used for destination verification.
        """
        hasher = xxhash.xxh64() if method == "xxHash (Fast)" else hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(self.CHUNK_SIZE):
                hasher.update(chunk)
        return hasher.hexdigest()
        
    def pause(self): self.is_paused = True
    def resume(self): self.is_paused = False
    def cancel(self): self.is_cancelled = True; self.is_paused = False
# --- END REFACTOR ---

# ... (MHLVerifyWorker and ReportWorker are unchanged)
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
            self.error.emit(error_msg, self.job['id'])
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