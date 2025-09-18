# main.py
import sys
import os
import json
from datetime import datetime
import multiprocessing # <-- IMPORT THIS

import psutil
import qtawesome as qta
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QFrame, QListWidget, QListWidgetItem,
    QComboBox, QProgressBar, QMessageBox, QCheckBox, QMenu, QInputDialog,
    QFileDialog, QTextEdit
)
from PySide6.QtCore import QTimer, QPoint, QUrl, Qt
from PySide6.QtGui import QIcon, QFont, QAction, QKeyEvent
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput

from config import APP_NAME, PROJECTS_BASE_DIR
from utils import resolve_path_template, format_bytes, format_eta
from ui_components import (
    ProjectManagerDialog, SettingsDialog, MetadataDialog, DropFrame, MHLVerifyDialog, JobListItem
)
from workers import EjectWorker, ScanWorker
from job_manager import JobManager
from report_manager import ReportManager

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME); self.setGeometry(100, 100, 1200, 800)
        self.setWindowIcon(qta.icon("fa5s.rocket"))
        
        self.project_path = None
        self.source_metadata = {}
        self.card_counter = 1
        self.naming_preset = {}
        self.global_settings = {}
        self.eject_worker = None
        self.scan_worker = None
        self.job_item_map = {}

        self.job_manager = JobManager(self)
        self.report_manager = ReportManager(self)

        self.player = QMediaPlayer()
        self.audio_output = QAudioOutput()
        self.player.setAudioOutput(self.audio_output)

        self._setup_ui()
        self._setup_menu()
        self._setup_drive_monitor()
        self._setup_sounds()
        self._connect_manager_signals()
        
        self.load_settings()

    def _setup_sounds(self):
        self.audio_output.setVolume(0.8)

    def _setup_ui(self):
        self.setStyleSheet("""
            QMainWindow { background-color: #1e1e1e; color: #dcdcdc; }
            QFrame { background-color: #252526; border-radius: 5px; }
            QLabel { color: #dcdcdc; }
            QPushButton { 
                background-color: #007acc; color: white; border: none; 
                padding: 8px 12px; border-radius: 4px;
            }
            QPushButton:hover { background-color: #005a9e; }
            QPushButton:disabled { background-color: #3e3e42; color: #888; }
            QPushButton[flat="true"] { background-color: transparent; }
            QListWidget { background-color: #2d2d30; border: 1px solid #3e3e42; border-radius: 4px; }
            QComboBox { 
                background-color: #3e3e42; padding: 5px; border-radius: 4px; 
            }
            QProgressBar { text-align: center; }
            QProgressBar::chunk { background-color: #007acc; }
            QMenu { background-color: #2d2d30; color: white; }
            QMenu::item:selected { background-color: #007acc; }
            QLineEdit { background-color: #3e3e42; border: 1px solid #555; padding: 5px; border-radius: 4px; }
            QCheckBox { spacing: 5px; }
            QCheckBox::indicator { width: 13px; height: 13px; }
            QGroupBox { border: 1px solid #3e3e42; margin-top: 10px; }
            QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top center; padding: 0 3px; }
            QTabWidget::pane { border: 1px solid #3e3e42; }
            QTabBar::tab { background: #252526; padding: 8px 20px; }
            QTabBar::tab:selected { background: #007acc; }
        """)
        
        main_widget = QWidget(); main_layout = QVBoxLayout(main_widget)
        top_layout = QHBoxLayout()
        self.source_frame = DropFrame("Sources"); self.dest_frame = DropFrame("Destinations")
        self.source_frame.path_list.metadata_requested.connect(self.show_metadata_dialog)
        top_layout.addWidget(self.source_frame); top_layout.addWidget(self.dest_frame)
        
        controls_frame = QFrame(); controls_layout = QVBoxLayout(controls_frame)
        
        row1_layout = QHBoxLayout()
        row1_layout.addWidget(QLabel("Checksum:")); self.checksum_combo = QComboBox()
        self.checksum_combo.addItems(["xxHash (Fast)", "MD5 (Compatible)"]); row1_layout.addWidget(self.checksum_combo)
        row1_layout.addStretch()
        
        self.mhl_verify_button = QPushButton(qta.icon("fa5s.check-double", color="white"), " Verify from MHL...")
        self.mhl_verify_button.clicked.connect(self.show_mhl_verify_dialog)
        row1_layout.addWidget(self.mhl_verify_button)

        self.settings_button = QPushButton(qta.icon("fa5s.cog", color="white"), "")
        self.settings_button.setToolTip("Open Settings")
        self.settings_button.setFixedSize(36, 36)
        self.settings_button.clicked.connect(self.show_settings_dialog)
        row1_layout.addWidget(self.settings_button)
        
        self.add_to_queue_button = QPushButton(qta.icon("fa5s.plus-circle", color="white"), " Add Job to Queue")
        self.add_to_queue_button.clicked.connect(self.add_job_to_queue)
        row1_layout.addWidget(self.add_to_queue_button)

        row2_layout = QHBoxLayout()
        self.create_source_folder_checkbox = QCheckBox("Create folder for each source"); self.create_source_folder_checkbox.setChecked(True)
        row2_layout.addWidget(self.create_source_folder_checkbox)
        self.eject_checkbox = QCheckBox("Eject on completion"); row2_layout.addWidget(self.eject_checkbox)
        self.skip_existing_checkbox = QCheckBox("Skip existing"); self.skip_existing_checkbox.setToolTip("Skips copying files that already exist with the same size at the destination."); self.skip_existing_checkbox.setChecked(True)
        row2_layout.addWidget(self.skip_existing_checkbox)
        self.resume_checkbox = QCheckBox("Resume partial"); self.resume_checkbox.setToolTip("Resumes copying for files that were partially transferred."); self.resume_checkbox.setChecked(True)
        row2_layout.addWidget(self.resume_checkbox)
        row2_layout.addStretch()

        controls_layout.addLayout(row1_layout)
        controls_layout.addLayout(row2_layout)

        # --- NEW: Global Progress Frame ---
        progress_frame = QFrame(); progress_layout = QVBoxLayout(progress_frame)
        progress_layout.setContentsMargins(10,5,10,5); progress_layout.setSpacing(5)
        
        self.overall_progress_bar = QProgressBar(); self.overall_progress_bar.setTextVisible(True)
        self.overall_progress_label = QLabel("Overall Progress");
        font = self.overall_progress_label.font(); font.setPointSize(font.pointSize() - 2); self.overall_progress_label.setFont(font)
        
        self.file_progress_bar = QProgressBar(); self.file_progress_bar.setFixedHeight(12)
        self.file_progress_label = QLabel("Current File"); self.file_progress_label.setFont(font)
        
        progress_layout.addWidget(self.overall_progress_label); progress_layout.addWidget(self.overall_progress_bar)
        progress_layout.addWidget(self.file_progress_label); progress_layout.addWidget(self.file_progress_bar)
        
        bottom_frame = QFrame(); bottom_layout = QVBoxLayout(bottom_frame)
        queue_controls_layout = QHBoxLayout()
        self.queue_title_label = QLabel("<b>Job Queue</b>")
        queue_controls_layout.addWidget(self.queue_title_label); queue_controls_layout.addStretch()
        
        self.start_queue_button = QPushButton(qta.icon("fa5s.play", color="white"), " Start Queue")
        self.remove_job_button = QPushButton(qta.icon("fa5s.minus-circle", color="white"), " Remove Job")
        self.cancel_button = QPushButton(qta.icon("fa5s.stop", color="white"), " Cancel")
        self.clear_completed_button = QPushButton(qta.icon("fa5s.trash", color="white"), " Clear Completed")
        self.save_session_report_button = QPushButton(qta.icon("fa5s.file-pdf", color="white"), " Save Session Report")

        queue_controls_layout.addWidget(self.start_queue_button)
        queue_controls_layout.addWidget(self.remove_job_button)
        queue_controls_layout.addWidget(self.cancel_button)
        queue_controls_layout.addWidget(self.clear_completed_button)
        queue_controls_layout.addWidget(self.save_session_report_button)

        self.job_list = QListWidget()
        self.job_list.setStyleSheet("QListWidget::item { border-bottom: 1px solid #3e3e42; }")

        bottom_layout.addLayout(queue_controls_layout)
        bottom_layout.addWidget(self.job_list)
        
        main_layout.addLayout(top_layout, 1)
        main_layout.addWidget(controls_frame)
        main_layout.addWidget(progress_frame)
        main_layout.addWidget(bottom_frame, 2)
        self.setCentralWidget(main_widget)
        
        self.source_frame.path_list.eject_requested.connect(self.on_eject_requested)
        self.dest_frame.path_list.eject_requested.connect(self.on_eject_requested)
    
    def _setup_menu(self):
        menu_bar = self.menuBar()
        file_menu = menu_bar.addMenu("&File")
        new_proj_action = QAction("New Project...", self); new_proj_action.triggered.connect(self.new_project); file_menu.addAction(new_proj_action)
        open_proj_action = QAction("Open Project...", self); open_proj_action.triggered.connect(self.open_project); file_menu.addAction(open_proj_action)
        self.recent_menu = QMenu("Open Recent", self); file_menu.addMenu(self.recent_menu)
        close_proj_action = QAction("Close Project", self); close_proj_action.triggered.connect(self.show_project_manager); file_menu.addAction(close_proj_action)
        file_menu.addSeparator()
        self.load_template_action = QAction("Load Job Template...", self)
        self.load_template_action.triggered.connect(self.load_job_template)
        file_menu.addAction(self.load_template_action)
        self.save_template_action = QAction("Save Job as Template...", self)
        self.save_template_action.triggered.connect(self.save_job_template)
        file_menu.addAction(self.save_template_action)
        file_menu.addSeparator()
        settings_action = QAction("Settings...", self); settings_action.triggered.connect(self.show_settings_dialog); file_menu.addAction(settings_action)
        file_menu.addSeparator()
        exit_action = QAction("Exit", self); exit_action.triggered.connect(self.close); file_menu.addAction(exit_action)
        self.load_template_action.setEnabled(False)
        self.save_template_action.setEnabled(False)

    def _setup_drive_monitor(self):
        self.drive_monitor_timer = QTimer(self); self.drive_monitor_timer.setInterval(3000)
        self.drive_monitor_timer.timeout.connect(self.check_drives)
    
    def _connect_manager_signals(self):
        self.job_manager.job_list_changed.connect(self.update_job_list)
        self.job_manager.queue_state_changed.connect(self.on_queue_state_changed)
        self.job_manager.overall_progress_updated.connect(self.update_overall_progress)
        self.job_manager.file_progress_updated.connect(self.update_file_progress)
        self.job_manager.ejection_requested.connect(self._show_ejection_dialog)
        self.job_manager.play_sound.connect(self.play_sound)
        self.job_manager.mhl_verify_report_ready.connect(self.show_mhl_verify_report)
        
        self.start_queue_button.clicked.connect(self.job_manager.start_or_pause_queue)
        self.cancel_button.clicked.connect(self.job_manager.cancel_queue)
        self.clear_completed_button.clicked.connect(self.job_manager.clear_completed_jobs)
        self.remove_job_button.clicked.connect(self.job_manager.remove_selected_job)
        
        self.save_session_report_button.clicked.connect(self.save_session_report)
        self.job_list.itemSelectionChanged.connect(self.update_remove_button_state)

    def on_queue_state_changed(self, is_running, job_queue):
        self._set_controls_enabled(not is_running)
        self.cancel_button.setVisible(is_running)
        self.start_queue_button.setEnabled(bool(job_queue) or is_running)

        if is_running:
            if self.job_manager.is_paused:
                self.start_queue_button.setText(" Resume"); self.start_queue_button.setIcon(qta.icon("fa5s.play", color="white"))
            else:
                self.start_queue_button.setText(" Pause"); self.start_queue_button.setIcon(qta.icon("fa5s.pause", color="white"))
        else:
            self.start_queue_button.setText(" Start Queue"); self.start_queue_button.setIcon(qta.icon("fa5s.play", color="white"))
            # Reset progress bars when queue stops
            self.update_overall_progress(0, "Queue Idle", 0.0, -1)
            self.update_file_progress(0, "Current File", "", 0.0)

    def update_overall_progress(self, percent, text, speed_mbps, eta_seconds):
        self.overall_progress_bar.setValue(percent)
        speed_text = f"{speed_mbps:.2f} MB/s"
        eta_text = f"ETA: {format_eta(eta_seconds)}"
        self.overall_progress_bar.setFormat(f"{percent}%")
        self.overall_progress_label.setText(f"{text} ({speed_text}, {eta_text})")

    def update_file_progress(self, percent, text, path, speed_mbps):
        self.file_progress_bar.setValue(percent)
        self.file_progress_label.setText(f"{text} {os.path.basename(path)}")
        self.file_progress_bar.setFormat(f"{percent}%")

    def play_sound(self, sound_type):
        if sound_type == "success":
            self.player.setSource(QUrl("qrc:/sounds/success.mp3"))
        elif sound_type == "error":
            self.player.setSource(QUrl("qrc:/sounds/error.mp3"))
        
        if self.player.source().isValid():
            self.player.play()
    
    def on_eject_requested(self, path):
        if self.eject_worker and self.eject_worker.isRunning(): return
        self.eject_worker = EjectWorker(path)
        self.eject_worker.ejection_finished.connect(self.on_ejection_finished)
        self.eject_worker.start()

    def on_ejection_finished(self, path, success):
        if success:
            QMessageBox.information(self, "Ejection Succeeded", f"Successfully ejected '{os.path.basename(path)}'.")
        else:
            QMessageBox.warning(self, "Ejection Failed", f"Failed to eject '{os.path.basename(path)}'. It may be in use by another application.")
        self.eject_worker = None

    def keyPressEvent(self, event: QKeyEvent):
        if self.job_manager.is_running: return
        if event.key() == Qt.Key_Backspace or event.key() == Qt.Key_Delete:
            focused_list = None
            if self.source_frame.path_list.hasFocus(): focused_list = self.source_frame.path_list
            elif self.dest_frame.path_list.hasFocus(): focused_list = self.dest_frame.path_list
            elif self.job_list.hasFocus():
                self.job_manager.remove_selected_job()
                return

            if focused_list and focused_list.currentItem():
                widget = focused_list.itemWidget(focused_list.currentItem())
                if widget: focused_list.remove_path(widget.path)

    def _set_controls_enabled(self, enabled):
        is_project_loaded = self.project_path is not None
        self.source_frame.setEnabled(enabled); self.dest_frame.setEnabled(enabled)
        self.checksum_combo.setEnabled(enabled)
        self.add_to_queue_button.setEnabled(enabled)
        self.mhl_verify_button.setEnabled(enabled and is_project_loaded)
        self.settings_button.setEnabled(enabled)
        self.start_queue_button.setEnabled(enabled and (bool(self.job_manager.job_queue) or bool(self.job_manager.active_workers)))
        self.eject_checkbox.setEnabled(enabled)
        self.skip_existing_checkbox.setEnabled(enabled)
        self.resume_checkbox.setEnabled(enabled)
        self.update_remove_button_state()
        self.update_folder_creation_mode()
        self.load_template_action.setEnabled(enabled and is_project_loaded)
        self.save_template_action.setEnabled(enabled and is_project_loaded)

    def update_folder_creation_mode(self):
        has_template = bool(self.naming_preset.get("template"))
        self.create_source_folder_checkbox.setEnabled(not has_template)
        if has_template: self.create_source_folder_checkbox.setChecked(False)

    def add_job_to_queue(self):
        if self.scan_worker and self.scan_worker.isRunning():
            return

        sources = self.source_frame.path_list.get_all_paths()
        destinations = self.dest_frame.path_list.get_all_paths()
        if not sources or not destinations:
            QMessageBox.warning(self, "Missing Paths", "Please add at least one source and one destination.")
            return

        job_params = {
            "sources": sources, "destinations": destinations, "naming_preset": self.naming_preset,
            "card_counter": self.card_counter, "has_template": bool(self.naming_preset.get("template")),
            "create_source_folder": self.create_source_folder_checkbox.isChecked(),
            "checksum_method": self.checksum_combo.currentText(), "eject_on_completion": self.eject_checkbox.isChecked(),
            "skip_existing": self.skip_existing_checkbox.isChecked(), "resume_partial": self.resume_checkbox.isChecked(),
            "metadata": self.source_metadata,
            "verification_mode": self.global_settings.get("verification_mode", "full"),
            "defer_post_process": self.global_settings.get("defer_post_process", False)
        }

        self.scan_worker = ScanWorker(job_params)
        self.scan_worker.scan_finished.connect(self.on_scan_finished)
        self.scan_worker.finished.connect(lambda: self._set_controls_enabled(True))
        
        self._set_controls_enabled(False)
        self.job_manager.job_list_changed.emit()
        self.scan_worker.start()

    def on_scan_finished(self, job_params):
        job_id = f"Job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(self.job_manager.get_all_jobs()) + 1}"
        
        destinations = set()
        for paths in job_params['resolved_dests'].values():
            if paths:
                common_base = os.path.dirname(os.path.commonpath(paths))
                destinations.add(common_base)

        job = {
            "id": job_id, "sources": job_params['sources'], "destinations": list(destinations),
            "resolved_dests": job_params['resolved_dests'], "checksum_method": job_params['checksum_method'],
            "status": "Queued", "eject_on_completion": job_params['eject_on_completion'],
            "skip_existing": job_params['skip_existing'], "resume_partial": job_params['resume_partial'],
            "metadata": job_params['metadata'],
            "verification_mode": job_params['verification_mode'],
            "defer_post_process": job_params['defer_post_process'],
            "report": {"total_size": job_params['total_size']} # Pre-populate for queue size calculation
        }
        
        self.job_manager.add_job_to_queue(job)
        self.card_counter += 1

    def update_remove_button_state(self):
        selected_items = self.job_list.selectedItems()
        can_remove = False
        if selected_items and not self.job_manager.is_running:
            item = self.job_list.item(self.job_list.row(selected_items[0]))
            widget = self.job_list.itemWidget(item)
            job_id_to_check = widget.job_id
            
            is_in_queue = any(job['id'] == job_id_to_check for job in self.job_manager.job_queue)
            if is_in_queue:
                can_remove = True
        self.remove_job_button.setEnabled(can_remove)

    def show_metadata_dialog(self, path):
        dialog = MetadataDialog(self.source_metadata.get(path), self)
        if dialog.exec(): self.source_metadata[path] = dialog.get_data()
        
    def check_drives(self):
        try: current_drives = set(p.mountpoint for p in psutil.disk_partitions())
        except Exception as e: print(f"Error getting disk partitions: {e}"); return
        new_drives, removed_drives = current_drives - self.mounted_drives, self.mounted_drives - current_drives
        if new_drives:
            for drive in new_drives:
                reply = QMessageBox.question(self, "New Drive Detected", f"New drive '{drive}' detected. Add it as a source?", QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
                if reply == QMessageBox.Yes: self.source_frame.path_list.add_path(drive)
        if removed_drives:
            all_paths = self.source_frame.path_list.get_all_paths() + self.dest_frame.path_list.get_all_paths()
            for drive in removed_drives:
                for path in all_paths:
                    if path.startswith(drive):
                        self.source_frame.path_list.remove_path(path)
                        self.dest_frame.path_list.remove_path(path)
        self.mounted_drives = current_drives

    def new_project(self):
        if self.project_path: self._save_project_state()
        project_name, ok = QInputDialog.getText(self, "New Project", "Enter Project Name:")
        if ok and project_name:
            if any(char in project_name for char in '/\\:*?"<>|'):
                QMessageBox.warning(self, "Invalid Name", "Project name contains invalid characters.")
                return
            new_project_path = os.path.join(PROJECTS_BASE_DIR, project_name)
            if os.path.exists(new_project_path):
                QMessageBox.warning(self, "Project Exists", "A project with this name already exists.")
                return
            try:
                os.makedirs(os.path.join(new_project_path, ".dit_project"))
                self._load_project(new_project_path)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Could not create project directory: {e}")

    def open_project(self):
        if self.project_path: self._save_project_state()
        path = QFileDialog.getExistingDirectory(self, "Select Project Folder", dir=PROJECTS_BASE_DIR)
        if path and os.path.isdir(os.path.join(path, ".dit_project")): self._load_project(path)
        elif path: QMessageBox.warning(self, "Invalid Project", "The selected folder is not a valid project.")
    
    def _load_project(self, path):
        if self.project_path: self._save_project_state()
        self.project_path = path; project_name = os.path.basename(path)
        self.setWindowTitle(f"{APP_NAME} - {project_name}")
        self.queue_title_label.setText(f"<b>Job Queue - {project_name}</b>")
        self.job_manager.job_queue.clear(); self.job_manager.completed_jobs.clear(); self.job_manager.post_process_queue.clear()
        self.card_counter = 1
        self._load_project_state()
        self._set_controls_enabled(True); self._add_to_recent_projects(path)
        self.mounted_drives = {p.mountpoint for p in psutil.disk_partitions()} 
        self.drive_monitor_timer.start()
        self.show()

    def _save_project_state(self):
        if not self.project_path: return
        def dt_handler(o):
            if isinstance(o, datetime): return o.isoformat()
        state = {"sources": self.source_frame.path_list.get_all_paths(), "destinations": self.dest_frame.path_list.get_all_paths(),
                 "checksum_method": self.checksum_combo.currentText(), "completed_jobs": self.job_manager.completed_jobs,
                 "source_metadata": self.source_metadata, "naming_preset": self.naming_preset, "card_counter": self.card_counter}
        state_path = os.path.join(self.project_path, ".dit_project", "project_state.json")
        try:
            with open(state_path, 'w') as f: json.dump(state, f, indent=2, default=dt_handler)
        except Exception as e: print(f"Error saving project state: {e}")

    def _load_project_state(self):
        state_path = os.path.join(self.project_path, ".dit_project", "project_state.json")
        self.source_frame.path_list.clear(); self.dest_frame.path_list.clear(); self.job_list.clear()
        if os.path.exists(state_path):
            try:
                with open(state_path, 'r') as f: state = json.load(f)
                for path in state.get("sources", []): self.source_frame.path_list.add_path(path)
                for path in state.get("destinations", []): self.dest_frame.path_list.add_path(path)
                self.checksum_combo.setCurrentText(state.get("checksum_method", "xxHash (Fast)"))
                self.source_metadata = state.get("source_metadata", {})
                self.naming_preset = state.get("naming_preset", {})
                self.card_counter = state.get("card_counter", 1)
                loaded_jobs = state.get("completed_jobs", [])
                for job in loaded_jobs:
                    if 'report' in job and job['report']:
                        if 'start_time' in job['report']: job['report']['start_time'] = datetime.fromisoformat(job['report']['start_time'])
                        if 'end_time' in job['report']: job['report']['end_time'] = datetime.fromisoformat(job['report']['end_time'])
                self.job_manager.completed_jobs = loaded_jobs
            except Exception as e: print(f"Error loading project state: {e}")
        self.update_job_list(); self.update_folder_creation_mode()

    def update_job_list(self):
        current_job_ids = {job['id'] for job in self.job_manager.get_all_jobs()}
        
        items_to_remove = []
        for job_id, widget in self.job_item_map.items():
            if job_id not in current_job_ids:
                items_to_remove.append(job_id)
            else:
                job_data = next((j for j in self.job_manager.get_all_jobs() if j['id'] == job_id), None)
                if job_data:
                    widget.update_status(job_data)

        for job_id in items_to_remove:
            for i in range(self.job_list.count()):
                item = self.job_list.item(i)
                widget = self.job_list.itemWidget(item)
                if widget and widget.job_id == job_id:
                    self.job_list.takeItem(i)
                    break
            del self.job_item_map[job_id]

        for job in self.job_manager.get_all_jobs():
            if job['id'] not in self.job_item_map:
                item = QListWidgetItem(self.job_list)
                job_widget = JobListItem(job)
                item.setSizeHint(job_widget.sizeHint())
                self.job_list.addItem(item)
                self.job_list.setItemWidget(item, job_widget)
                self.job_item_map[job['id']] = job_widget
        
        self.job_list.setContextMenuPolicy(Qt.CustomContextMenu); self.job_list.customContextMenuRequested.connect(self.show_job_context_menu)
        self.clear_completed_button.setVisible(bool(self.job_manager.completed_jobs))
        self.save_session_report_button.setVisible(bool(self.job_manager.completed_jobs))
        self.update_remove_button_state()
        
    def show_job_context_menu(self, pos: QPoint):
        item = self.job_list.itemAt(pos)
        if not item: return
        widget = self.job_list.itemWidget(item)
        if not widget: return
        
        job_data = next((j for j in self.job_manager.get_all_jobs() if j['id'] == widget.job_id), None)
        if not job_data: return

        menu = QMenu(self)
        if job_data in self.job_manager.completed_jobs and 'report' in job_data:
            menu.addAction("Save PDF Report", lambda: self.report_manager.save_pdf_report(job_data['report']))
            if job_data.get("job_type") != "mhl_verify":
                menu.addAction("Save MHL Manifest", lambda: self.report_manager.save_mhl_manifest(job_data['report']))
                menu.addAction("Save CSV Log", lambda: self.report_manager.save_csv_log(job_data['report']))
            menu.addSeparator()

        if job_data.get('status') == 'Completed' and job_data.get("job_type") != "mhl_verify":
            menu.addAction("Run Post-Processing", lambda: self.job_manager.run_post_process_for_job(job_data))

        if menu.actions(): menu.exec(self.job_list.mapToGlobal(pos))
            
    def _show_ejection_dialog(self, sources):
        source_names = "\n".join([f"- {os.path.basename(p)}" for p in sources])
        reply = QMessageBox.question(self, "Eject Sources?", f"The following sources were verified successfully and can be ejected. Eject them now?\n\n{source_names}", QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
        if reply == QMessageBox.Yes:
            for path in sources: self.on_eject_requested(path)

    def show_mhl_verify_report(self, report_data):
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("MHL Verification Issues")
        msg_box.setIcon(QMessageBox.Warning)
        
        summary = (f"Verification completed with {report_data['failed_count']} failed checksum(s) "
                   f"and {report_data['missing_count']} missing file(s).")
        
        details = ""
        failed_files = [f for f in report_data['files'] if f['status'] == 'FAILED']
        missing_files = [f for f in report_data['files'] if f['status'] == 'Missing']

        if failed_files:
            details += "<b>Failed Checksums:</b>\n"
            for f in failed_files:
                details += f"• {os.path.basename(f['path'])}\n"
        
        if missing_files:
            details += "\n<b>Missing Files:</b>\n"
            for f in missing_files:
                details += f"• {os.path.basename(f['path'])}\n"
        
        msg_box.setText(summary)
        msg_box.setInformativeText("See details below. A full PDF report can also be saved.")
        
        text_edit = QTextEdit()
        text_edit.setHtml(details)
        text_edit.setReadOnly(True)
        text_edit.setMinimumHeight(150)
        
        grid_layout = msg_box.layout()
        grid_layout.addWidget(text_edit, grid_layout.rowCount(), 0, 1, grid_layout.columnCount())

        msg_box.exec()

    def save_session_report(self):
        if not self.job_manager.completed_jobs:
            QMessageBox.information(self, "No Jobs", "There are no completed jobs to report.")
            return
        
        copy_jobs = [j for j in self.job_manager.completed_jobs if j.get("job_type", "copy") == "copy"]
        if not copy_jobs:
            QMessageBox.information(self, "No Copy Jobs", "Session reports can only be generated for copy jobs.")
            return

        consolidated_report = {'job_id': f"SESSION_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                               'start_time': copy_jobs[0]['report']['start_time'],
                               'end_time': copy_jobs[-1]['report']['end_time'],
                               'sources': list(set(s for j in copy_jobs for s in j['report']['sources'])),
                               'destinations': list(set(d for j in copy_jobs for d in j['report']['destinations'])),
                               'checksum_method': copy_jobs[0]['report']['checksum_method'],
                               'files': [f for j in copy_jobs for f in j['report']['files']],
                               'status': 'Session Complete',
                               'total_size': sum(j['report']['total_size'] for j in copy_jobs)}
        self.report_manager.save_pdf_report(consolidated_report)

    def get_settings_path(self): return os.path.join(PROJECTS_BASE_DIR, "settings.json")
    
    def save_settings(self):
        settings = {"global": self.global_settings, "recent_projects": getattr(self, "recent_projects", [])}
        os.makedirs(PROJECTS_BASE_DIR, exist_ok=True)
        with open(self.get_settings_path(), "w") as f: json.dump(settings, f, indent=2)
        
    def load_settings(self):
        settings_path = self.get_settings_path()
        if os.path.exists(settings_path):
            try:
                with open(settings_path, "r") as f: settings = json.load(f)
                self.global_settings = settings.get("global", {})
                self.job_manager.set_max_concurrent_jobs(self.global_settings.get("concurrent_jobs", 1))
                self.recent_projects = settings.get("recent_projects", [])
                self._populate_recent_menu()
                
                last_project = self.global_settings.get("last_project")
                if last_project and os.path.exists(last_project): self._load_project(last_project)
                else: self.show_project_manager()
                    
            except json.JSONDecodeError: self.show_project_manager()
        else: self.show_project_manager()

    def show_settings_dialog(self):
        is_project_loaded = self.project_path is not None
        dialog = SettingsDialog(self.global_settings, self.naming_preset, is_project_loaded, self)
        if dialog.exec():
            updated_settings = dialog.get_settings()
            self.global_settings = updated_settings["global"]
            self.job_manager.set_max_concurrent_jobs(self.global_settings.get("concurrent_jobs", 1))
            self.save_settings()

            if is_project_loaded:
                self.naming_preset = updated_settings["naming_preset"]
                self.update_folder_creation_mode()
                self._save_project_state()

    def show_project_manager(self):
        if self.project_path:
            self._save_project_state()
            self.global_settings["last_project"] = None
            self.save_settings()
            self.project_path = None
        self.hide()
        recent_projects = getattr(self, "recent_projects", [])
        dialog = ProjectManagerDialog(recent_projects, self)
        dialog.project_selected.connect(self._load_project)
        dialog.new_project_requested.connect(self.new_project)
        if not dialog.exec():
             if not self.project_path: sys.exit()
                
    def _add_to_recent_projects(self, path):
        if not hasattr(self, "recent_projects"): self.recent_projects = []
        if path in self.recent_projects: self.recent_projects.remove(path)
        self.recent_projects.insert(0, path); self.recent_projects = self.recent_projects[:5]
        self.global_settings["last_project"] = path
        self._populate_recent_menu()
        self.save_settings()
        
    def _populate_recent_menu(self):
        self.recent_menu.clear()
        if hasattr(self, "recent_projects") and self.recent_projects:
            for path in self.recent_projects:
                action = QAction(os.path.basename(path), self); action.setData(path)
                action.triggered.connect(self._open_recent_project); self.recent_menu.addAction(action)
        self.recent_menu.setEnabled(bool(self.recent_menu.actions()))
        
    def _open_recent_project(self):
        if self.project_path: self._save_project_state()
        action = self.sender()
        if action:
            path = action.data()
            if os.path.exists(path): self._load_project(path)
            else:
                QMessageBox.warning(self, "Project Not Found", "The project path could not be found. It may have been moved or deleted.")
                self.recent_projects.remove(path); self._populate_recent_menu()

    def show_mhl_verify_dialog(self):
        dialog = MHLVerifyDialog(self)
        dialog.add_job_requested.connect(self.on_mhl_job_add_requested)
        dialog.exec()

    def on_mhl_job_add_requested(self, mhl_path, target_dir):
        job_id = f"Job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(self.job_manager.get_all_jobs()) + 1}"
        job = {
            "id": job_id,
            "job_type": "mhl_verify",
            "mhl_file": mhl_path,
            "target_dir": target_dir,
            "status": "Queued"
        }
        self.job_manager.add_job_to_queue(job)

    def save_job_template(self):
        default_name = f"{os.path.basename(self.project_path or 'Untitled')}_Template.dittemplate"
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Job Template", default_name, "DIT Templates (*.dittemplate)")
        if not file_path:
            return

        template_data = {
            "destinations": self.dest_frame.path_list.get_all_paths(),
            "checksum_method": self.checksum_combo.currentText(),
            "create_source_folder": self.create_source_folder_checkbox.isChecked(),
            "eject_on_completion": self.eject_checkbox.isChecked(),
            "skip_existing": self.skip_existing_checkbox.isChecked(),
            "resume_partial": self.resume_checkbox.isChecked()
        }

        try:
            with open(file_path, 'w') as f:
                json.dump(template_data, f, indent=2)
            QMessageBox.information(self, "Success", "Job template saved successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not save template: {e}")

    def load_job_template(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Load Job Template", "", "DIT Templates (*.dittemplate)")
        if not file_path:
            return

        try:
            with open(file_path, 'r') as f:
                template_data = json.load(f)

            self.source_frame.path_list.clear()
            self.dest_frame.path_list.clear()

            for path in template_data.get("destinations", []):
                self.dest_frame.path_list.add_path(path)
            
            self.checksum_combo.setCurrentText(template_data.get("checksum_method", "xxHash (Fast)"))
            self.create_source_folder_checkbox.setChecked(template_data.get("create_source_folder", True))
            self.eject_checkbox.setChecked(template_data.get("eject_on_completion", False))
            self.skip_existing_checkbox.setChecked(template_data.get("skip_existing", True))
            self.resume_checkbox.setChecked(template_data.get("resume_partial", True))
            
            QMessageBox.information(self, "Success", "Job template loaded. Please add your source drives.")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not load template: {e}")

    def closeEvent(self, event):
        if self.project_path: self._save_project_state()
        if self.job_manager.is_running:
            reply = QMessageBox.question(self, "Exit Confirmation", "A transfer is in progress. Are you sure you want to exit?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply == QMessageBox.Yes:
                self.job_manager.cancel_queue()
                event.accept()
            else: event.ignore()
        else: event.accept()

if __name__ == '__main__':
    # --- START MODIFICATION ---
    # Necessary for PyInstaller and multiprocessing on macOS/Windows
    multiprocessing.freeze_support()
    # --- END MODIFICATION ---

    if not os.path.exists(PROJECTS_BASE_DIR): os.makedirs(PROJECTS_BASE_DIR)
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = MainWindow()
    sys.exit(app.exec())