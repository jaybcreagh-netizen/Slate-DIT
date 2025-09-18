# job_manager.py
import time
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QMessageBox

from workers import TransferWorker, PostProcessWorker, MHLVerifyWorker

class JobManager(QObject):
    job_list_changed = Signal()
    queue_state_changed = Signal(bool, list)
    
    # --- NEW/MODIFIED SIGNALS for Global UI ---
    overall_progress_updated = Signal(int, str, float, int)
    file_progress_updated = Signal(int, str, str, float)
    # ---
    
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
        self.max_concurrent_jobs = 1
        self.is_running = False
        self.is_paused = False
        self.current_queue_had_errors = False
        
        # --- NEW Attributes for aggregation ---
        self.total_queue_size = 0
        self.total_bytes_processed_in_queue = 0
        self.queue_start_time = 0
        self.active_job_progress = {} # {job_id: bytes_processed}

    def set_max_concurrent_jobs(self, count):
        self.max_concurrent_jobs = count

    def get_all_jobs(self):
        active_jobs = [worker.job for worker in self.active_workers]
        return self.job_queue + active_jobs + self.completed_jobs

    def clear_completed_jobs(self):
        self.completed_jobs.clear()
        self.job_list_changed.emit()

    def add_job_to_queue(self, job):
        self.job_queue.append(job)
        self.job_list_changed.emit()
        if self.is_running:
            self._start_available_jobs()

    def remove_selected_job(self):
        if self.is_running: return
        selected_items = self.window.job_list.selectedItems()
        if not selected_items: return
        item = self.window.job_list.item(self.window.job_list.row(selected_items[0]))
        widget = self.window.job_list.itemWidget(item)
        job_id_to_remove = widget.job_id
        
        original_len = len(self.job_queue)
        self.job_queue = [job for job in self.job_queue if job['id'] != job_id_to_remove]
        
        if len(self.job_queue) < original_len:
             self.job_list_changed.emit()
             self.queue_state_changed.emit(self.is_running, self.job_queue)

    def start_or_pause_queue(self):
        if not self.is_running:
            self.current_queue_had_errors = False
            self.is_running = True
            self.is_paused = False
            
            # --- Initialize aggregation state ---
            copy_jobs = [j for j in self.job_queue if j.get("job_type", "copy") == "copy"]
            self.total_queue_size = sum(j['report']['total_size'] for j in copy_jobs)
            self.total_bytes_processed_in_queue = 0
            self.active_job_progress = {}
            self.queue_start_time = time.monotonic()
            
            self.queue_state_changed.emit(self.is_running, self.job_queue)
            self._start_available_jobs()
        else:
            if self.is_paused:
                self.is_paused = False
                for worker in self.active_workers: worker.resume()
            else:
                self.is_paused = True
                for worker in self.active_workers: worker.pause()
            self.queue_state_changed.emit(self.is_running, self.job_queue)

    def cancel_queue(self):
        if not self.is_running: return
        reply = QMessageBox.question(self.window, "Cancel Queue", "Are you sure you want to cancel all running and queued jobs?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            for worker in self.active_workers: worker.cancel()
            self.job_queue.clear()
            self.is_running = False
            self.is_paused = False

    def _start_available_jobs(self):
        while len(self.active_workers) < self.max_concurrent_jobs and self.job_queue:
            job = self.job_queue.pop(0)
            job['status'] = 'Running'
            job_id = job['id']
            
            job_type = job.get("job_type", "copy")
            if job_type == "mhl_verify":
                worker = MHLVerifyWorker(job)
                worker.progress.connect(lambda p, t, s, e, jid=job_id: self.overall_progress_updated.emit(p, f"Verifying MHL: {t}", s, e))
            else: # copy job
                worker = TransferWorker(job, self.window.project_path)
                self.active_job_progress[job_id] = 0
                worker.progress.connect(self._on_worker_progress_updated)
            
            worker.file_progress.connect(self.file_progress_updated.emit)
            worker.job_finished.connect(self.on_job_finished)
            worker.error.connect(lambda msg: self.file_progress_updated.emit(0, f"ERROR: {msg}", "", 0.0))
            worker.finished.connect(lambda w=worker: self._on_worker_finished(w))
            
            self.active_workers.append(worker)
            self.job_list_changed.emit()
            worker.start()

    def _on_worker_progress_updated(self, job_id, bytes_processed_in_job, speed_mbps, eta_seconds):
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
        if worker in self.active_workers:
            self.active_workers.remove(worker)
        
        if self.is_running:
            self._start_available_jobs()
        
        if not self.job_queue and not self.active_workers:
            self.queue_finished()

    def on_job_finished(self, report_data):
        job_id = report_data['job_id']
        
        finished_worker = next((w for w in self.active_workers if w.job['id'] == job_id), None)
        if not finished_worker: return

        found_job = finished_worker.job
        self.completed_jobs.append(found_job)
        
        # --- Update overall progress accurately ---
        if found_job.get("job_type", "copy") == "copy":
            bytes_processed_before_finish = self.active_job_progress.pop(job_id, 0)
            delta = report_data.get('total_size', 0) - bytes_processed_before_finish
            self.total_bytes_processed_in_queue += delta
            
        found_job['status'] = report_data['status']
        found_job['report'] = report_data
        
        status_lower = report_data.get('status', '').lower()
        if 'error' in status_lower or 'failed' in status_lower:
            self.current_queue_had_errors = True
            self.play_sound.emit("error")
        elif not self.window.global_settings.get("defer_post_process", False) and found_job.get("job_type", "copy") == "copy":
            self.post_process_queue.append(found_job)
            self._start_post_processing_if_needed()

        self.job_list_changed.emit()
        
        if found_job.get("job_type") == "mhl_verify" and report_data.get('status') == 'Completed with issues':
            self.mhl_verify_report_ready.emit(report_data)
        
        ejectable_sources = report_data.get('ejectable_sources_on_success', [])
        if ejectable_sources:
            self.ejection_requested.emit(ejectable_sources)

    def queue_finished(self):
        # Only play success sound if the queue was running and finished without being cancelled
        if self.is_running and not self.current_queue_had_errors:
            self.play_sound.emit("success")
            self.overall_progress_updated.emit(100, "Queue completed", 0.0, 0)

        self.is_running = False
        self.is_paused = False
        self.queue_state_changed.emit(self.is_running, self.job_queue)
        
        # Reset progress after a short delay
        # QTimer.singleShot(3000, lambda: self.overall_progress_updated.emit(0, "Overall Progress", 0.0, -1))
        # QTimer.singleShot(3000, lambda: self.file_progress_updated.emit(0, "Current File", "", 0.0))

    def run_post_process_for_job(self, job_data):
        if job_data:
            self.post_process_queue.append(job_data)
            self._start_post_processing_if_needed()

    def _start_post_processing_if_needed(self):
        if hasattr(self, 'post_process_worker') and self.post_process_worker.isRunning(): return
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