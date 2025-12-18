# job_manager.py
import time
import os
import queue
from datetime import datetime
from collections import deque
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QMessageBox

from workers import TransferWorker, PostProcessWorker, MHLVerifyWorker, ScanWorker

class JobManager(QObject):
    job_list_changed = Signal()
    queue_state_changed = Signal(bool, list)
    
    overall_progress_updated = Signal(int, str, float, int)
    job_file_progress_updated = Signal(str, int, str, str, float)
    
    post_process_status_updated = Signal(str)
    ejection_requested = Signal(list)
    play_sound = Signal(str)
    mhl_verify_report_ready = Signal(dict)

    def __init__(self, window):
        super().__init__()
        self.window = window
        self.job_queue = []
        self.completed_jobs = []
        self.post_process_queue = []
        self.active_workers = []
        self.scan_worker = None
        self.max_concurrent_jobs = 1
        self.is_running = False
        self.is_paused = False
        self.current_queue_had_errors = False
        
        self.total_queue_size = 0
        self.total_bytes_processed_in_queue = 0
        self.queue_start_time = 0
        self.active_job_progress = {}

        # --- NEW: Attributes for rolling average speed calculation ---
        self.speed_history = deque(maxlen=20) # Store last 20 data points (time, bytes)
        self.last_progress_update_time = 0

    def create_job_from_ui(self):
        # Allow starting a new job even if another is scanning, but for simplicity
        # we still restrict one scan at a time if the UI depends on it (which it does slightly).
        if self.scan_worker and self.scan_worker.isRunning():
            return
        
        sources = self.window.source_frame.path_list.get_all_paths()
        destinations = self.window.dest_frame.path_list.get_all_paths()

        if not sources or not destinations:
            QMessageBox.warning(self.window, "Missing Paths", "Please add at least one source and one destination.")
            return

        file_queue = queue.Queue()
        job_id = f"Job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(self.get_all_jobs()) + 1}"

        job_params = {
            "sources": sources,
            "destinations": destinations,
            "naming_preset": self.window.naming_preset,
            "card_counter": self.window.card_counter,
            "has_template": bool(self.window.naming_preset.get("template")),
            "create_source_folder": self.window.create_source_folder_checkbox.isChecked(),
            "checksum_method": self.window.checksum_combo.currentText(),
            "eject_on_completion": self.window.eject_checkbox.isChecked(),
            "skip_existing": self.window.skip_existing_checkbox.isChecked(),
            "resume_partial": self.window.resume_checkbox.isChecked(),
            "metadata": self.window.source_metadata,
            "verification_mode": self.window.global_settings.get("verification_mode", "full"),
            "defer_post_process": self.window.global_settings.get("defer_post_process", False)
        }

        # Create the Job object immediately
        job = {
            "id": job_id,
            "sources": sources,
            "destinations": destinations,
            "resolved_dests": {}, # Will be populated on scan finish
            "checksum_method": job_params['checksum_method'],
            "status": "Scanning",
            "eject_on_completion": job_params['eject_on_completion'],
            "skip_existing": job_params['skip_existing'],
            "resume_partial": job_params['resume_partial'],
            "metadata": job_params['metadata'],
            "verification_mode": job_params['verification_mode'],
            "defer_post_process": job_params['defer_post_process'],
            "report": {"total_size": 0}, # Initial size is 0
            "file_queue": file_queue, # Runtime object
        }

        self.scan_worker = ScanWorker(job_params, file_queue)
        self.scan_worker.scan_progress.connect(lambda f, s: self.on_scan_progress(job, f, s))
        self.scan_worker.scan_finished.connect(lambda params: self.on_scan_finished_update_job(job, params))

        # We don't block controls anymore, or maybe we do just for simplicity of "adding" jobs?
        # The user requested scalability. Non-blocking UI is key.
        # But we must ensure the user doesn't change naming presets while scan is running for THIS job.
        # For now, let's keep controls disabled during the "add" phase, but we want the transfer to start ASAP.
        # Actually, "Add to Queue" usually just adds it. If we auto-start, we need `start_or_pause_queue` logic.
        
        self.add_job_to_queue(job)
        self.window.card_counter += 1
        self.scan_worker.start()

        # Explicitly start the queue if it's not already running.
        # This ensures the TransferWorker picks up the new job (which is in "Scanning" state) immediately.
        # We might need to adjust `_start_available_jobs` to pick up "Scanning" jobs too.
        if not self.is_running:
             self.start_or_pause_queue()

    def on_scan_progress(self, job, files_found, current_total_size):
        if 'report' in job:
            job['report']['total_size'] = current_total_size
        # Force update total queue size if this job is running/queued
        if self.is_running:
             copy_jobs = [j for j in self.job_queue if j.get("job_type", "copy") == "copy"]
             # Recalculate total queue size from ALL jobs, as this one is updating
             current_queue_total = sum(j['report']['total_size'] for j in copy_jobs)
             self.total_queue_size = current_queue_total + sum(j['report']['total_size'] for j in self.active_workers if j.get("job_type", "copy") == "copy")

    def on_scan_finished_update_job(self, job, job_params):
        # Update the job record with final scan details (for report/restart)
        job['resolved_dests'] = job_params['resolved_dests']
        job['report']['total_size'] = job_params['total_size']
        job['all_source_files'] = job_params['all_source_files']

        # If the job hasn't started yet (unlikely if queue is running), update status
        if job['status'] == "Scanning":
            job['status'] = "Queued"

        self.job_list_changed.emit()

    def set_max_concurrent_jobs(self, count):
        self.max_concurrent_jobs = count

    def get_all_jobs(self):
        active_jobs = [worker.job for worker in self.active_workers]
        return active_jobs + self.job_queue + self.completed_jobs

    def clear_completed_jobs(self):
        self.completed_jobs.clear()
        self.job_list_changed.emit()

    def add_job_to_queue(self, job):
        self.job_queue.append(job)
        self.job_list_changed.emit()
        if self.is_running:
            self._start_available_jobs()

    def remove_job_by_id(self, job_id_to_remove):
        if self.is_running:
            QMessageBox.warning(self.window, "Cannot Remove Job", "Jobs cannot be removed while the queue is running.")
            return
        initial_len = len(self.job_queue)
        self.job_queue = [job for job in self.job_queue if job['id'] != job_id_to_remove]
        if len(self.job_queue) < initial_len:
            self.job_list_changed.emit()
            self.queue_state_changed.emit(self.is_running, self.job_queue)
            return
        initial_len = len(self.completed_jobs)
        self.completed_jobs = [job for job in self.completed_jobs if job['id'] != job_id_to_remove]
        if len(self.completed_jobs) < initial_len:
            self.job_list_changed.emit()
            return

    def start_or_pause_queue(self):
        if not self.is_running:
            if not self.job_queue:
                return
            self.current_queue_had_errors = False
            self.is_running = True
            self.is_paused = False
            copy_jobs = [j for j in self.job_queue if j.get("job_type", "copy") == "copy"]
            self.total_queue_size = sum(j['report']['total_size'] for j in copy_jobs if 'report' in j and 'total_size' in j['report'])
            self.total_bytes_processed_in_queue = 0
            self.active_job_progress = {}
            # --- REFACTOR: Reset speed calculation history ---
            self.speed_history.clear()
            self.queue_start_time = time.monotonic()
            self.last_progress_update_time = self.queue_start_time
            self.queue_state_changed.emit(self.is_running, self.job_queue)
            self._start_available_jobs()
        else:
            if self.is_paused:
                self.is_paused = False
                for worker in self.active_workers:
                    worker.resume()
                # Un-pause timers
                self.queue_start_time += (time.monotonic() - self.pause_time)
                self.last_progress_update_time += (time.monotonic() - self.pause_time)
            else:
                self.is_paused = True
                self.pause_time = time.monotonic()
                for worker in self.active_workers:
                    worker.pause()
            self.queue_state_changed.emit(self.is_running, self.job_queue)

    def cancel_queue(self):
        if not self.is_running:
            return
        reply = QMessageBox.question(self.window, "Cancel Queue", "Are you sure you want to cancel all running and queued jobs?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            for worker in self.active_workers[:]:
                worker.cancel()
                job = worker.job
                job['status'] = 'Cancelled'
                self.job_queue.insert(0, job)
            for job in self.job_queue:
                if job['status'] != 'Cancelled':
                    job['status'] = 'Cancelled'
            self.is_running = False
            self.is_paused = False
            self.job_list_changed.emit()
            self.queue_state_changed.emit(self.is_running, self.job_queue)

    def _start_available_jobs(self):
        while len(self.active_workers) < self.max_concurrent_jobs and self.job_queue:
            job = self.job_queue.pop(0)
            if job['status'] == 'Cancelled':
                self.completed_jobs.append(job)
                continue

            # Allow "Scanning" jobs to start running (TransferWorker will consume from queue)
            # If it's "Queued" or "Scanning", it's valid to start.
            if job['status'] == "Scanning":
                 # Keep it as "Scanning" or change to "Running"?
                 # If we change to "Running", the UI icon updates.
                 # The ScanWorker is still running in background.
                 job['status'] = 'Running'
            else:
                 job['status'] = 'Running'

            job_id = job['id']
            job_type = job.get("job_type", "copy")
            if job_type == "mhl_verify":
                worker = MHLVerifyWorker(job)
                worker.progress.connect(lambda p, t, s, e, jid=job_id: self.overall_progress_updated.emit(p, f"Verifying MHL: {t}", s, e))
            else:
                worker = TransferWorker(job, self.window.project_path)
                self.active_job_progress[job_id] = 0
                worker.progress.connect(self._on_worker_progress_updated)
            worker.file_progress.connect(
                lambda p, t, path, s, jid=job_id: self.job_file_progress_updated.emit(jid, p, t, path, s)
            )
            worker.job_finished.connect(self.on_job_finished)
            worker.error.connect(lambda msg, jid=job_id: self.job_file_progress_updated.emit(jid, 0, f"ERROR: {msg}", "", 0.0))
            worker.finished.connect(lambda w=worker: self._on_worker_finished(w))
            self.active_workers.append(worker)
            self.job_list_changed.emit()
            worker.start()

    # --- START REFACTOR: Implement rolling average calculation ---
    def _on_worker_progress_updated(self, job_id, bytes_processed_in_job, speed_mbps, eta_seconds):
        if job_id not in self.active_job_progress:
            return
        
        # Calculate how many new bytes were processed since the last update
        delta = bytes_processed_in_job - self.active_job_progress[job_id]
        if delta <= 0:
            return # No change, no update needed

        self.total_bytes_processed_in_queue += delta
        self.active_job_progress[job_id] = bytes_processed_in_job

        # Update speed history for rolling average
        current_time = time.monotonic()
        self.speed_history.append((current_time, self.total_bytes_processed_in_queue))

        # Prune old history points (older than 10 seconds)
        while self.speed_history and current_time - self.speed_history[0][0] > 10:
            self.speed_history.popleft()

        # Calculate rolling average speed
        overall_speed_mbps = 0.0
        if len(self.speed_history) > 1:
            time_delta = self.speed_history[-1][0] - self.speed_history[0][0]
            byte_delta = self.speed_history[-1][1] - self.speed_history[0][1]
            if time_delta > 0.001:
                speed_bps = byte_delta / time_delta
                overall_speed_mbps = speed_bps / (1024 * 1024)

        # Calculate ETA based on the rolling average speed
        bytes_remaining = self.total_queue_size - self.total_bytes_processed_in_queue
        overall_eta_seconds = bytes_remaining / (overall_speed_mbps * 1024 * 1024) if overall_speed_mbps > 0 else -1

        # Calculate overall percentage
        percent = int((self.total_bytes_processed_in_queue / self.total_queue_size) * 100) if self.total_queue_size > 0 else 0
        
        text = f"Processing queue... ({len(self.active_workers)} active jobs)"
        self.overall_progress_updated.emit(percent, text, overall_speed_mbps, overall_eta_seconds)
    # --- END REFACTOR ---

    def _on_worker_finished(self, worker):
        if worker in self.active_workers:
            self.active_workers.remove(worker)
        if self.is_running:
            self._start_available_jobs()
        if not self.job_queue and not self.active_workers:
            self.queue_finished()

    def on_job_finished(self, report_data):
        job_id = report_data['job_id']
        finished_worker_job = None
        for worker in self.active_workers:
            if worker.job['id'] == job_id:
                finished_worker_job = worker.job
                break
        if not finished_worker_job:
            return

        # --- START REFACTOR: Remove flawed "true-up" logic ---
        # The worker's progress signals are now the single source of truth.
        # This prevents over-counting and negative ETAs.
        self.active_job_progress.pop(job_id, 0)
        # --- END REFACTOR ---
        
        self.completed_jobs.append(finished_worker_job)
        finished_worker_job['status'] = report_data['status']
        finished_worker_job['report'] = report_data

        status_lower = report_data.get('status', '').lower()
        if 'error' in status_lower or 'failed' in status_lower:
            self.current_queue_had_errors = True
            self.play_sound.emit("error")
        elif not self.window.global_settings.get("defer_post_process", False) and finished_worker_job.get("job_type", "copy") == "copy":
            self.post_process_queue.append(finished_worker_job)
            self._start_post_processing_if_needed()
        self.job_list_changed.emit()
        if finished_worker_job.get("job_type") == "mhl_verify" and report_data.get('status') == 'Completed with issues':
            self.mhl_verify_report_ready.emit(report_data)
        ejectable_sources = report_data.get('ejectable_sources_on_success', [])
        if ejectable_sources:
            self.ejection_requested.emit(ejectable_sources)

    def queue_finished(self):
        if self.is_running:
             if not self.current_queue_had_errors:
                self.play_sound.emit("success")
                self.overall_progress_updated.emit(100, "Queue completed", 0.0, 0)
             else:
                self.overall_progress_updated.emit(100, "Queue completed with errors", 0.0, 0)
        self.is_running = False
        self.is_paused = False
        self.queue_state_changed.emit(self.is_running, self.job_queue)
        
    def run_post_process_for_job(self, job_data):
        if job_data:
            self.post_process_queue.append(job_data)
            self._start_post_processing_if_needed()

    def _start_post_processing_if_needed(self):
        if hasattr(self, 'post_process_worker') and self.post_process_worker.isRunning():
            return
        if not self.post_process_queue:
            self.post_process_status_updated.emit("")
            return
        next_job = self.post_process_queue.pop(0)
        next_job['status'] = 'Post-processing'
        self.job_list_changed.emit()
        self.post_process_worker = PostProcessWorker(next_job, self.window.project_path)
        self.post_process_worker.progress.connect(lambda cur, tot, name: self.post_process_status_updated.emit(f"Post-processing: {name} ({cur}/{tot})"))
        self.post_process_worker.file_processed.connect(self._on_file_processed)
        self.post_process_worker.job_processed.connect(self._on_job_processed)
        self.post_process_worker.start()

    def _on_file_processed(self, job_id, source_path, updates):
        for job in self.completed_jobs:
            if job['id'] == job_id:
                if 'files' in job['report']:
                    for file_info in job['report']['files']:
                        if file_info['source'] == source_path:
                            file_info.update(updates)
                            break
                break

    def _on_job_processed(self, job_id):
        for job in self.completed_jobs:
            if job['id'] == job_id:
                job['status'] = 'Processed'
                break
        self.job_list_changed.emit()
        self._start_post_processing_if_needed()