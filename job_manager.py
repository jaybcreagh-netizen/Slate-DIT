# job_manager.py
import time
import os # <-- MODIFICATION: Import os for path checks
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QMessageBox

from workers import TransferWorker, PostProcessWorker, MHLVerifyWorker
from models import Job, JobStatus

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
        self.job_queue: list[Job] = []
        self.completed_jobs: list[Job] = []
        self.post_process_queue: list[Job] = []
        self.active_workers = []
        self.max_concurrent_jobs = 1
        self.is_running = False
        self.is_paused = False
        self.current_queue_had_errors = False
        
        self.total_queue_size = 0
        self.total_bytes_processed_in_queue = 0
        self.queue_start_time = 0
        self.active_job_progress = {}

    def set_max_concurrent_jobs(self, count):
        self.max_concurrent_jobs = count

    def get_all_jobs(self) -> list[Job]:
        active_jobs = [worker.job for worker in self.active_workers]
        return active_jobs + self.job_queue + self.completed_jobs

    def clear_completed_jobs(self):
        self.completed_jobs.clear()
        self.job_list_changed.emit()

    def add_job_to_queue(self, job: Job):
        self.job_queue.append(job)
        self.job_list_changed.emit()
        if self.is_running:
            self._start_available_jobs()

    def remove_job_by_id(self, job_id_to_remove: str):
        if self.is_running:
            QMessageBox.warning(self.window, "Cannot Remove Job", "Jobs cannot be removed while the queue is running.")
            return

        initial_len_queue = len(self.job_queue)
        self.job_queue = [job for job in self.job_queue if job.id != job_id_to_remove]
        if len(self.job_queue) < initial_len_queue:
            self.job_list_changed.emit()
            self.queue_state_changed.emit(self.is_running, self.job_queue)
            return

        initial_len_completed = len(self.completed_jobs)
        self.completed_jobs = [job for job in self.completed_jobs if job.id != job_id_to_remove]
        if len(self.completed_jobs) < initial_len_completed:
            self.job_list_changed.emit()
            return

    def start_or_pause_queue(self):
        if not self.is_running:
            if not self.job_queue:
                return

            # --- MODIFICATION: Pre-flight check before starting ---
            if not self._validate_paths():
                return

            self.current_queue_had_errors = False
            self.is_running = True
            self.is_paused = False
            
            copy_jobs = [j for j in self.job_queue if j.job_type == "copy"]
            self.total_queue_size = sum(j.report.get('total_size', 0) for j in copy_jobs)
            self.total_bytes_processed_in_queue = 0
            self.active_job_progress = {}
            self.queue_start_time = time.monotonic()
            
            self.queue_state_changed.emit(self.is_running, self.job_queue)
            self._start_available_jobs()
        else:
            self.is_paused = not self.is_paused
            for worker in self.active_workers:
                if self.is_paused:
                    worker.pause()
                else:
                    worker.resume()
            self.queue_state_changed.emit(self.is_running, self.job_queue)

    def _validate_paths(self) -> bool:
        """
        Checks if all source and destination paths for jobs in the queue exist
        before starting the transfer. Returns False if any path is missing.
        """
        all_sources = set()
        all_destinations = set()

        for job in self.job_queue:
            if job.job_type == 'copy':
                all_sources.update(job.sources)
                all_destinations.update(job.destinations)
        
        missing_paths = []
        for path in list(all_sources) + list(all_destinations):
            if not os.path.exists(path):
                missing_paths.append(path)
        
        if missing_paths:
            msg = "The following paths could not be found. Please re-add them or remove the corresponding jobs before starting the queue:\n\n"
            msg += "\n".join(f"- {p}" for p in missing_paths)
            QMessageBox.critical(self.window, "Paths Missing", msg)
            return False
        
        return True

    def cancel_queue(self):
        # ... (unchanged)
        if not self.is_running: return
        reply = QMessageBox.question(self.window, "Cancel Queue", "Are you sure you want to cancel all running and queued jobs?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            for worker in self.active_workers[:]:
                worker.cancel()
                job = worker.job
                job.status = JobStatus.CANCELLED
                self.completed_jobs.append(job)
            self.active_workers.clear()
            for job in self.job_queue:
                job.status = JobStatus.CANCELLED
                self.completed_jobs.append(job)
            self.job_queue.clear()
            self.is_running = False
            self.is_paused = False
            self.job_list_changed.emit()
            self.queue_state_changed.emit(self.is_running, self.job_queue)

    def _start_available_jobs(self):
        # ... (unchanged)
        while len(self.active_workers) < self.max_concurrent_jobs and self.job_queue:
            job = self.job_queue.pop(0)
            job.status = JobStatus.RUNNING
            worker = None
            if job.job_type == "mhl_verify":
                worker = MHLVerifyWorker(job)
                worker.progress.connect(lambda p, t, s, e, jid=job.id: self.overall_progress_updated.emit(p, f"Verifying MHL: {t}", s, e))
            else:
                worker = TransferWorker(job, self.window.project_path)
                self.active_job_progress[job.id] = 0
                worker.progress.connect(self._on_worker_progress_updated)
            worker.file_progress.connect(lambda p, t, path, s, jid=job.id: self.job_file_progress_updated.emit(jid, p, t, path, s))
            worker.job_finished.connect(self.on_job_finished)
            worker.error.connect(lambda msg, jid=job.id: self.job_file_progress_updated.emit(jid, 0, f"ERROR: {msg}", "", 0.0))
            worker.finished.connect(lambda w=worker: self._on_worker_finished(w))
            self.active_workers.append(worker)
            self.job_list_changed.emit()
            worker.start()

    def _on_worker_progress_updated(self, job_id, bytes_processed_in_job, speed_mbps, eta_seconds):
        # ... (unchanged)
        if job_id not in self.active_job_progress: return
        delta = bytes_processed_in_job - self.active_job_progress[job_id]
        self.total_bytes_processed_in_queue += delta
        self.active_job_progress[job_id] = bytes_processed_in_job
        percent = int((self.total_bytes_processed_in_queue / self.total_queue_size) * 100) if self.total_queue_size > 0 else 0
        elapsed_time = time.monotonic() - self.queue_start_time
        overall_speed_mbps = (self.total_bytes_processed_in_queue / (1024*1024)) / elapsed_time if elapsed_time > 0 else 0
        bytes_remaining = self.total_queue_size - self.total_bytes_processed_in_queue
        overall_eta_seconds = bytes_remaining / (overall_speed_mbps * 1024 * 1024) if overall_speed_mbps > 0 else -1
        text = f"Processing queue... ({len(self.active_workers)} active jobs)"
        self.overall_progress_updated.emit(percent, text, overall_speed_mbps, overall_eta_seconds)

    def _on_worker_finished(self, worker):
        # ... (unchanged)
        if worker in self.active_workers: self.active_workers.remove(worker)
        if self.is_running: self._start_available_jobs()
        if not self.job_queue and not self.active_workers: self.queue_finished()

    def on_job_finished(self, finished_job: Job):
        # ... (unchanged)
        self.completed_jobs.append(finished_job)
        if finished_job.job_type == "copy":
            bytes_processed_before_finish = self.active_job_progress.pop(finished_job.id, 0)
            delta = finished_job.report.get('total_size', 0) - bytes_processed_before_finish
            self.total_bytes_processed_in_queue += delta
        if finished_job.status == JobStatus.COMPLETED_WITH_ERRORS:
            self.current_queue_had_errors = True
            self.play_sound.emit("error")
        elif not finished_job.defer_post_process and finished_job.job_type == "copy":
            self.post_process_queue.append(finished_job)
            self._start_post_processing_if_needed()
        self.job_list_changed.emit()
        if finished_job.job_type == "mhl_verify" and finished_job.report.get('status') == 'Completed with issues':
            self.mhl_verify_report_ready.emit(finished_job.report)
        if ejectable_sources := finished_job.report.get('ejectable_sources_on_success', []):
            self.ejection_requested.emit(ejectable_sources)

    def queue_finished(self):
        # ... (unchanged)
        if self.is_running:
             if not self.current_queue_had_errors:
                self.play_sound.emit("success")
                self.overall_progress_updated.emit(100, "Queue completed", 0.0, 0)
             else:
                self.overall_progress_updated.emit(100, "Queue completed with errors", 0.0, 0)
        self.is_running = False
        self.is_paused = False
        self.queue_state_changed.emit(self.is_running, self.job_queue)
        
    def run_post_process_for_job(self, job: Job):
        # ... (unchanged)
        if job:
            self.post_process_queue.append(job)
            self._start_post_processing_if_needed()

    def _start_post_processing_if_needed(self):
        # ... (unchanged)
        if hasattr(self, 'post_process_worker') and self.post_process_worker.isRunning(): return
        if not self.post_process_queue:
            self.post_process_status_updated.emit(""); return
        next_job = self.post_process_queue.pop(0)
        next_job.status = JobStatus.POST_PROCESSING
        self.job_list_changed.emit()
        self.post_process_worker = PostProcessWorker(next_job, self.window.project_path)
        self.post_process_worker.progress.connect(lambda cur, tot, name: self.post_process_status_updated.emit(f"Post-processing: {name} ({cur}/{tot})"))
        self.post_process_worker.file_processed.connect(self._on_file_processed)
        self.post_process_worker.job_processed.connect(self._on_job_processed)
        self.post_process_worker.start()

    def _on_file_processed(self, job_id, source_path, updates):
        # ... (unchanged)
        for job in self.completed_jobs:
            if job.id == job_id:
                if 'files' in job.report:
                    for file_info in job.report['files']:
                        if file_info['source'] == source_path:
                            file_info.update(updates); break
                break

    def _on_job_processed(self, job_id):
        # ... (unchanged)
        for job in self.completed_jobs:
            if job.id == job_id:
                job.status = JobStatus.PROCESSED; break
        self.job_list_changed.emit()
        self._start_post_processing_if_needed()