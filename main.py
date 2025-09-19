# main.py
import sys
import os
import json
from datetime import datetime
import multiprocessing
import tempfile
import atexit
import shutil

import psutil
import qtawesome as qta
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QFrame, QListWidget, QListWidgetItem,
    QComboBox, QProgressBar, QMessageBox, QMenu, QInputDialog,
    QFileDialog, QTextEdit, QStatusBar, QToolBar, QSizePolicy, QSplitter
)
from PySide6.QtCore import QTimer, QPoint, QUrl, Qt, QSize, QFile
from PySide6.QtGui import QIcon, QFont, QAction, QKeyEvent
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput

import resources_rc

from config import APP_NAME, PROJECTS_BASE_DIR
from utils import get_icon, resolve_path_template, format_bytes, format_eta
# --- MODIFICATION: Removed StyledComboBox from import ---
from ui_components import (
    ProjectManagerDialog, SettingsDialog, MetadataDialog, DropFrame, MHLVerifyDialog, JobListItem, ToggleSwitch
)
from workers import EjectWorker, ScanWorker
from job_manager import JobManager
from report_manager import ReportManager
from models import Job, JobStatus

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME); self.setGeometry(100, 100, 1200, 800)
        self.setWindowIcon(get_icon("tray.and.arrow.down.fill", "fa5s.rocket"))
        
        self.project_path = None; self.source_metadata = {}; self.card_counter = 1
        self.naming_preset = {}; self.global_settings = {}
        self.eject_worker = None; self.scan_worker = None
        self.job_item_map = {}
        self.job_manager = JobManager(self)
        self.report_manager = ReportManager(self)
        self.player = QMediaPlayer(); self.audio_output = QAudioOutput()
        self.player.setAudioOutput(self.audio_output)
        
        self._setup_ui()
        self._setup_menu()
        self._setup_drive_monitor()
        self._setup_sounds()
        self._connect_manager_signals()
        
        self.load_settings()

    def _setup_sounds(self): self.audio_output.setVolume(0.8)

    def _setup_ui(self):
        if sys.platform == "darwin": self.setUnifiedTitleAndToolBarOnMac(True)
        self.setStyleSheet("""
            QMainWindow { background-color: #1e1e1e; }
            QWidget { font-family: ".AppleSystemUIFont", "Segoe UI", "Roboto", "Helvetica Neue", sans-serif; color: #f0f0f0; font-size: 13px; }
            QFrame#MainFrame { background-color: transparent; border: none; }
            QFrame#DropFrame { background-color: #2c2c2e; border: 1.5px dashed #444; border-radius: 8px; }
            QSplitter::handle { background-color: #3a3a3c; height: 1px; }
            QSplitter::handle:hover { background-color: #4a4a4c; }
            QSplitter::handle:pressed { background-color: #007aff; }
            QLabel#TitleLabel { font-size: 15px; font-weight: bold; padding: 4px 0; background-color: transparent; border: none;}
            QPushButton { background-color: #007aff; color: white; border: none; padding: 8px 14px; border-radius: 6px; font-weight: 600; }
            QPushButton:hover { background-color: #1085e0; }
            QPushButton:disabled { background-color: #3e3e42; color: #777; }
            QPushButton#ToolbarButton { background-color: #3e3e42; }
            QPushButton#ToolbarButton:hover { background-color: #4e4e50; }
            QToolBar { border: none; padding: 5px; margin: 0; }
            
            /* --- FINAL ComboBox Styling for ALL QComboBoxes --- */
            QComboBox { 
                background-color: #333336; /* Indent effect */
                padding: 6px 20px 6px 10px; /* Right padding for arrow */
                border-radius: 5px; 
                border: 1px solid #555; 
                combobox-popup: 0;
            }
            QComboBox:hover {
                background-color: #3e3e42; /* Brighten on hover */
            }
            QComboBox::drop-down { 
                border: none; 
                subcontrol-origin: padding; 
                subcontrol-position: top right; 
                width: 15px; /* Tighter spacing */
            }
            QComboBox::down-arrow { 
                image: url(:/fonts/chevron.down.svg); 
                width: 12px; 
                height: 12px;
            }
            QComboBox QAbstractItemView { 
                background-color: #3c3c3e; 
                border: 1px solid #555; 
                color: white; 
                selection-background-color: #007aff; 
                padding: 4px;
            }
            
            QProgressBar { text-align: center; border-radius: 5px; border: 1px solid #444; color: white; }
            QProgressBar::chunk { background-color: #007aff; border-radius: 4px; }
            QProgressBar[complete="true"]::chunk { background-color: #4CAF50; }
            QMenu { background-color: #3c3c3e; color: white; border: 1px solid #555;}
            QMenu::item:selected { background-color: #007aff; }
            QGroupBox { border: 1px solid #444; border-radius: 6px; margin-top: 6px; }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; }
        """)
        
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget); main_layout.setContentsMargins(0, 0, 0, 0); main_layout.setSpacing(0)
        self.setCentralWidget(main_widget)
        self._setup_toolbar()
        
        content_frame = QFrame(objectName="MainFrame")
        content_layout = QVBoxLayout(content_frame); content_layout.setContentsMargins(10, 10, 10, 10); content_layout.setSpacing(10)
        main_layout.addWidget(content_frame)

        splitter = QSplitter(Qt.Vertical)
        top_panel = QWidget()
        top_layout = QHBoxLayout(top_panel); top_layout.setContentsMargins(0,0,0,0); top_layout.setSpacing(10)
        self.source_frame = DropFrame("Sources"); self.dest_frame = DropFrame("Destinations")
        self.source_frame.path_list.metadata_requested.connect(self.show_metadata_dialog)
        top_layout.addWidget(self.source_frame); top_layout.addWidget(self.dest_frame)
        splitter.addWidget(top_panel)
        
        bottom_panel = QWidget()
        bottom_layout = QVBoxLayout(bottom_panel); bottom_layout.setContentsMargins(0,0,0,0); bottom_layout.setSpacing(10)
        options_frame = QFrame(); options_frame.setStyleSheet("border: none; background-color: #2c2c2e; border-radius: 8px;")
        options_main_layout = QVBoxLayout(options_frame); options_main_layout.setContentsMargins(10, 5, 10, 10)
        options_main_layout.addWidget(QLabel("<b>Transfer Options</b>", objectName="TitleLabel"))
        
        options_grid_layout = QHBoxLayout(); options_grid_layout.setSpacing(15)
        
        # Use a standard QComboBox, it will be styled by the stylesheet
        self.checksum_combo = QComboBox()
        self.checksum_combo.addItem(get_icon("hare.fill", "fa5s.rocket"), "xxHash (Fast)")
        self.checksum_combo.addItem(get_icon("tortoise.fill", "fa5s.shield-alt"), "MD5 (Compatible)")
        self.checksum_combo.setToolTip("xxHash is significantly faster. MD5 is older but widely supported.")
        self.checksum_combo.setItemData(0, "xxHash (Fast)")
        self.checksum_combo.setItemData(1, "MD5 (Compatible)")

        options_grid_layout.addWidget(QLabel("Checksum:"));
        options_grid_layout.addWidget(self.checksum_combo); options_grid_layout.addStretch(1)
        self.create_source_folder_checkbox = ToggleSwitch(); self.create_source_folder_checkbox.setChecked(True)
        options_grid_layout.addWidget(self.create_source_folder_checkbox); options_grid_layout.addWidget(QLabel("Create source folder")); options_grid_layout.addSpacing(10)
        self.eject_checkbox = ToggleSwitch(); options_grid_layout.addWidget(self.eject_checkbox); options_grid_layout.addWidget(QLabel("Eject on completion")); options_grid_layout.addSpacing(10)
        self.skip_existing_checkbox = ToggleSwitch(); self.skip_existing_checkbox.setChecked(True)
        options_grid_layout.addWidget(self.skip_existing_checkbox); options_grid_layout.addWidget(QLabel("Skip existing")); options_grid_layout.addSpacing(10)
        self.resume_checkbox = ToggleSwitch(); self.resume_checkbox.setChecked(True)
        options_grid_layout.addWidget(self.resume_checkbox); options_grid_layout.addWidget(QLabel("Resume partial"))
        options_main_layout.addLayout(options_grid_layout); bottom_layout.addWidget(options_frame)
        
        job_queue_frame = QFrame(); job_queue_frame.setStyleSheet("background-color: #2c2c2e; border-radius: 8px;")
        queue_layout = QVBoxLayout(job_queue_frame); queue_layout.setContentsMargins(0, 0, 0, 0)
        self.job_list = QListWidget(); self.job_list.setStyleSheet("QListWidget { background-color: transparent; border: none; } QListWidget::item { border-bottom: 1px solid #3a3a3c; } QListWidget::item:hover { background-color: #38383a; border-radius: 5px; } QListWidget::item:selected { background-color: #404043; border-radius: 5px; border-bottom: 1px solid transparent; }")
        queue_layout.addWidget(self.job_list); bottom_layout.addWidget(job_queue_frame, 1); splitter.addWidget(bottom_panel); content_layout.addWidget(splitter)
        splitter.setSizes([self.height() * 0.35, self.height() * 0.65])
        self.setStatusBar(QStatusBar(self)); self.statusBar().hide()
        self.source_frame.path_list.eject_requested.connect(self.on_eject_requested)
        self.dest_frame.path_list.eject_requested.connect(self.on_eject_requested)

    # ... (all other methods from __init__ down are unchanged and can be copied from the previous version)

    def _setup_toolbar(self):
        self.toolbar = QToolBar("Main Toolbar"); self.toolbar.setMovable(False); self.addToolBar(Qt.ToolBarArea.TopToolBarArea, self.toolbar)
        self.add_to_queue_button = QPushButton(get_icon("plus", "fa5s.plus", color="white"), " Add Job")
        self.start_queue_button = QPushButton(get_icon("play.fill", "fa5s.play", color="white"), " Start Queue")
        self.cancel_button = QPushButton(get_icon("stop.fill", "fa5s.stop", color="white"), " Cancel")
        self.toolbar.addWidget(self.add_to_queue_button); self.toolbar.addWidget(self.start_queue_button); self.toolbar.addWidget(self.cancel_button)
        spacer = QWidget(); spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding); self.toolbar.addWidget(spacer)
        progress_widget = QWidget(); progress_layout = QVBoxLayout(progress_widget); progress_layout.setContentsMargins(0,0,0,0); progress_layout.setSpacing(2)
        
        file_progress_layout = QHBoxLayout()
        self.file_progress_label = QLabel("Idle"); 
        font = self.file_progress_label.font(); font.setPointSize(font.pointSize() - 2); self.file_progress_label.setFont(font)
        file_progress_layout.addStretch()
        file_progress_layout.addWidget(self.file_progress_label)
        file_progress_layout.addStretch()
        
        self.overall_progress_bar = QProgressBar(); self.overall_progress_bar.setTextVisible(True); self.overall_progress_bar.setMinimumWidth(350)
        
        progress_layout.addLayout(file_progress_layout);
        progress_layout.addWidget(self.overall_progress_bar)
        self.toolbar.addWidget(progress_widget)
        
        spacer2 = QWidget(); spacer2.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding); self.toolbar.addWidget(spacer2)
        self.queue_title_label = QLabel("<b>Job Queue</b>"); self.toolbar.addWidget(self.queue_title_label)
        spacer3 = QWidget(); spacer3.setFixedWidth(10); self.toolbar.addWidget(spacer3)
        self.mhl_verify_button = QPushButton(get_icon("checkmark.shield.fill", "fa5s.check-double"), " MHL Verify")
        self.settings_button = QPushButton(get_icon("gear", "fa5s.cog"), "")
        for btn in [self.mhl_verify_button, self.settings_button, self.cancel_button]: btn.setObjectName("ToolbarButton")
        self.toolbar.addWidget(self.mhl_verify_button); self.toolbar.addWidget(self.settings_button)

    def show_status_message(self, message, timeout=5000): self.statusBar().show(); self.statusBar().showMessage(message, timeout)
    def clear_status_message(self): self.statusBar().clearMessage(); self.statusBar().hide()
    
    def _setup_menu(self):
        menu_bar = self.menuBar(); file_menu = menu_bar.addMenu("&File")
        new_proj_action = QAction("New Project...", self); new_proj_action.triggered.connect(self.new_project); file_menu.addAction(new_proj_action)
        open_proj_action = QAction("Open Project...", self); open_proj_action.triggered.connect(self.open_project); file_menu.addAction(open_proj_action)
        self.recent_menu = QMenu("Open Recent", self); file_menu.addMenu(self.recent_menu)
        close_proj_action = QAction("Close Project", self); close_proj_action.triggered.connect(self.show_project_manager); file_menu.addAction(close_proj_action)
        file_menu.addSeparator()
        self.load_template_action = QAction("Load Job Template...", self); self.load_template_action.triggered.connect(self.load_job_template); file_menu.addAction(self.load_template_action)
        self.save_template_action = QAction("Save Job as Template...", self); self.save_template_action.triggered.connect(self.save_job_template); file_menu.addAction(self.save_template_action)
        file_menu.addSeparator()
        settings_action = QAction("Settings...", self); settings_action.triggered.connect(self.show_settings_dialog); file_menu.addAction(settings_action)
        file_menu.addSeparator()
        exit_action = QAction("Exit", self); exit_action.triggered.connect(self.close); file_menu.addAction(exit_action)
        self.load_template_action.setEnabled(False); self.save_template_action.setEnabled(False)

    def _setup_drive_monitor(self):
        self.drive_monitor_timer = QTimer(self); self.drive_monitor_timer.setInterval(3000)
        self.drive_monitor_timer.timeout.connect(self.check_drives)
    
    def _connect_manager_signals(self):
        self.job_manager.job_list_changed.connect(self.update_job_list)
        self.job_manager.queue_state_changed.connect(self.on_queue_state_changed)
        self.job_manager.overall_progress_updated.connect(self.update_overall_progress)
        self.job_manager.job_file_progress_updated.connect(self.update_job_file_progress)
        self.job_manager.ejection_requested.connect(self._show_ejection_dialog)
        self.job_manager.play_sound.connect(self.play_sound)
        self.job_manager.mhl_verify_report_ready.connect(self.show_mhl_verify_report)
        self.add_to_queue_button.clicked.connect(self.add_job_to_queue)
        self.start_queue_button.clicked.connect(self.job_manager.start_or_pause_queue)
        self.cancel_button.clicked.connect(self.job_manager.cancel_queue)
        self.mhl_verify_button.clicked.connect(self.show_mhl_verify_dialog)
        self.settings_button.clicked.connect(self.show_settings_dialog)

    def on_queue_state_changed(self, is_running, job_queue):
        self._set_controls_enabled(not is_running)
        self.cancel_button.setVisible(is_running)
        self.start_queue_button.setEnabled(bool(job_queue) or is_running)
        if is_running:
            icon_name = "pause.fill" if not self.job_manager.is_paused else "play.fill"
            fa_name = "fa5s.pause" if not self.job_manager.is_paused else "fa5s.play"
            text = " Pause" if not self.job_manager.is_paused else " Resume"
            self.start_queue_button.setText(text); self.start_queue_button.setIcon(get_icon(icon_name, fa_name, color="white"))
        else:
            self.start_queue_button.setText(" Start Queue"); self.start_queue_button.setIcon(get_icon("play.fill", "fa5s.play", color="white"))
            self.update_overall_progress(0, "Queue Idle", 0.0, -1)
            self.file_progress_label.setText("Idle")

    def update_overall_progress(self, percent, text, speed_mbps, eta_seconds):
        self.overall_progress_bar.setValue(percent)
        is_complete = (percent == 100 and "complet" in text.lower())
        self.overall_progress_bar.setProperty("complete", is_complete); self.overall_progress_bar.style().polish(self.overall_progress_bar)
        if is_complete:
            self.overall_progress_bar.setFormat(text); self.file_progress_label.setText("Complete")
        else:
            self.overall_progress_bar.setFormat(f"{percent}% ({speed_mbps:.2f} MB/s, ETA: {format_eta(eta_seconds)})")
        
    def update_job_file_progress(self, job_id, percent, text, path, speed_mbps):
        active_job = self.job_manager.active_workers[0].job if self.job_manager.active_workers else None
        if active_job and active_job.id == job_id:
            if speed_mbps > 0: self.file_progress_label.setText(f"{os.path.basename(path)} - {percent}% ({speed_mbps:.2f} MB/s)")
            elif path: self.file_progress_label.setText(f"{os.path.basename(path)} - {text}")
            else: self.file_progress_label.setText("Waiting...")

    def play_sound(self, sound_type: str):
        if sound_type == "success": self.player.setSource(QUrl("qrc:/sounds/success.mp3"))
        elif sound_type == "error": self.player.setSource(QUrl("qrc:/sounds/error.mp3"))
        if self.player.source().isValid(): self.player.play()
    
    def on_eject_requested(self, path):
        if self.eject_worker and self.eject_worker.isRunning(): return
        self.eject_worker = EjectWorker(path); self.eject_worker.ejection_finished.connect(self.on_ejection_finished); self.eject_worker.start()

    def on_ejection_finished(self, path, success):
        if success: QMessageBox.information(self, "Ejection Succeeded", f"Successfully ejected '{os.path.basename(path)}'.")
        else: QMessageBox.warning(self, "Ejection Failed", f"Failed to eject '{os.path.basename(path)}'. It may be in use by another application.")
        self.eject_worker = None

    def keyPressEvent(self, event: QKeyEvent):
        if self.job_manager.is_running: return
        if event.key() in [Qt.Key_Backspace, Qt.Key_Delete]:
            focused_list = None
            if self.source_frame.path_list.hasFocus(): focused_list = self.source_frame.path_list
            elif self.dest_frame.path_list.hasFocus(): focused_list = self.dest_frame.path_list
            elif self.job_list.hasFocus():
                if item := self.job_list.currentItem():
                    if widget := self.job_list.itemWidget(item): self.job_manager.remove_job_by_id(widget.job_id)
                return
            if focused_list and (item := focused_list.currentItem()):
                if widget := focused_list.itemWidget(item): focused_list.remove_path(widget.path)

    def _set_controls_enabled(self, enabled):
        is_project_loaded = self.project_path is not None
        self.source_frame.setEnabled(enabled); self.dest_frame.setEnabled(enabled)
        self.checksum_combo.setEnabled(enabled); self.add_to_queue_button.setEnabled(enabled)
        self.start_queue_button.setEnabled(enabled and (bool(self.job_manager.job_queue) or bool(self.job_manager.active_workers)))
        self.eject_checkbox.setEnabled(enabled); self.skip_existing_checkbox.setEnabled(enabled); self.resume_checkbox.setEnabled(enabled)
        self.update_folder_creation_mode()
        self.load_template_action.setEnabled(enabled and is_project_loaded); self.save_template_action.setEnabled(enabled and is_project_loaded)
        self.settings_button.setEnabled(enabled); self.mhl_verify_button.setEnabled(enabled and is_project_loaded)
        for action in self.menuBar().actions():
            if action.text() == "&File":
                for file_action in action.menu().actions():
                    if file_action.text() in ["Settings...", "New Project...", "Open Project...", "Open Recent", "Close Project"]: file_action.setEnabled(enabled)

    def update_folder_creation_mode(self):
        has_template = bool(self.naming_preset.get("template"))
        self.create_source_folder_checkbox.setEnabled(not has_template)
        if has_template: self.create_source_folder_checkbox.setChecked(False)

    def add_job_to_queue(self):
        if self.scan_worker and self.scan_worker.isRunning(): return
        sources = self.source_frame.path_list.get_all_paths(); destinations = self.dest_frame.path_list.get_all_paths()
        if not sources or not destinations:
            QMessageBox.warning(self, "Missing Paths", "Please add at least one source and one destination."); return
        
        checksum_method = self.checksum_combo.currentData()
        job_params = {
            "sources": sources, "destinations": destinations, "naming_preset": self.naming_preset,
            "card_counter": self.card_counter, "has_template": bool(self.naming_preset.get("template")),
            "create_source_folder": self.create_source_folder_checkbox.isChecked(),
            "checksum_method": checksum_method
        }
        self.show_status_message(f"Scanning {len(sources)} source(s)...", 0)
        self.scan_worker = ScanWorker(job_params)
        self.scan_worker.scan_finished.connect(self.on_scan_finished)
        self.scan_worker.finished.connect(lambda: (self._set_controls_enabled(True), self.clear_status_message()))
        self._set_controls_enabled(False); self.scan_worker.start()

    def on_scan_finished(self, job_params):
        new_job = Job(
            sources=job_params['sources'],
            destinations=list(set(os.path.dirname(os.path.commonpath(p)) for p in job_params['resolved_dests'].values() if p)),
            resolved_dests=job_params['resolved_dests'],
            checksum_method=job_params['checksum_method'],
            eject_on_completion=self.eject_checkbox.isChecked(),
            skip_existing=self.skip_existing_checkbox.isChecked(),
            resume_partial=self.resume_checkbox.isChecked(),
            metadata=self.source_metadata,
            verification_mode=self.global_settings.get("verification_mode", "full"),
            defer_post_process=self.global_settings.get("defer_post_process", False),
            report={"total_size": job_params['total_size']}
        )
        self.job_manager.add_job_to_queue(new_job)
        self.card_counter += 1

    def show_metadata_dialog(self, path):
        dialog = MetadataDialog(self.source_metadata.get(path), self);
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
            if any(c in project_name for c in '/\\:*?"<>|'):
                QMessageBox.warning(self, "Invalid Name", "Project name contains invalid characters."); return
            new_project_path = os.path.join(PROJECTS_BASE_DIR, project_name)
            if os.path.exists(new_project_path):
                QMessageBox.warning(self, "Project Exists", "A project with this name already exists."); return
            try: os.makedirs(os.path.join(new_project_path, ".dit_project")); self._load_project(new_project_path)
            except Exception as e: QMessageBox.critical(self, "Error", f"Could not create project directory: {e}")

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
        self._load_project_state(); self._set_controls_enabled(True); self._add_to_recent_projects(path)
        try: self.mounted_drives = {p.mountpoint for p in psutil.disk_partitions()}; self.drive_monitor_timer.start()
        except Exception as e: print(f"Could not start drive monitor: {e}")
        self.show()

    def _save_project_state(self):
        if not self.project_path: return
        state = { "sources": self.source_frame.path_list.get_all_paths(), "destinations": self.dest_frame.path_list.get_all_paths(), "checksum_method": self.checksum_combo.currentData(), "completed_jobs": [job.to_dict() for job in self.job_manager.completed_jobs], "source_metadata": self.source_metadata, "naming_preset": self.naming_preset, "card_counter": self.card_counter }
        state_path = os.path.join(self.project_path, ".dit_project", "project_state.json")
        try:
            with open(state_path, 'w') as f: json.dump(state, f, indent=2)
        except Exception as e: print(f"Error saving project state: {e}")

    def _load_project_state(self):
        state_path = os.path.join(self.project_path, ".dit_project", "project_state.json")
        self.source_frame.path_list.clear(); self.dest_frame.path_list.clear(); self.job_list.clear()
        if os.path.exists(state_path):
            try:
                with open(state_path, 'r') as f: state = json.load(f)
                for path in state.get("sources", []): self.source_frame.path_list.add_path(path)
                for path in state.get("destinations", []): self.dest_frame.path_list.add_path(path)
                checksum_value = state.get("checksum_method", "xxHash (Fast)")
                index = self.checksum_combo.findData(checksum_value)
                if index != -1: self.checksum_combo.setCurrentIndex(index)
                
                self.source_metadata = state.get("source_metadata", {})
                self.naming_preset = state.get("naming_preset", {})
                self.card_counter = state.get("card_counter", 1)
                self.job_manager.completed_jobs = [Job.from_dict(job_data) for job_data in state.get("completed_jobs", [])]
            except Exception as e: print(f"Error loading project state: {e}")
        self.update_job_list(); self.update_folder_creation_mode()

    def update_job_list(self):
        current_job_ids = {job.id for job in self.job_manager.get_all_jobs()}
        items_to_remove = []
        for job_id in list(self.job_item_map.keys()):
            if job_id not in current_job_ids:
                items_to_remove.append(job_id)
            else:
                job = next((j for j in self.job_manager.get_all_jobs() if j.id == job_id), None)
                if job: self.job_item_map[job_id].update_status(job)
        for job_id in items_to_remove:
            for i in range(self.job_list.count()):
                item = self.job_list.item(i)
                if item and self.job_list.itemWidget(item).job_id == job_id:
                    self.job_list.takeItem(i); del self.job_item_map[job_id]; break
        for job in self.job_manager.get_all_jobs():
            if job.id not in self.job_item_map:
                item = QListWidgetItem(self.job_list)
                job_widget = JobListItem(job)
                job_widget.remove_requested.connect(self.job_manager.remove_job_by_id)
                item.setSizeHint(job_widget.sizeHint())
                self.job_list.addItem(item); self.job_list.setItemWidget(item, job_widget)
                self.job_item_map[job.id] = job_widget
        self.job_list.setContextMenuPolicy(Qt.CustomContextMenu); self.job_list.customContextMenuRequested.connect(self.show_job_context_menu)
        
    def show_job_context_menu(self, pos: QPoint):
        item = self.job_list.itemAt(pos);
        if not item: return
        widget = self.job_list.itemWidget(item)
        job = widget.job
        menu = QMenu(self); menu.setAttribute(Qt.WA_DeleteOnClose)
        if job.status in [JobStatus.COMPLETED, JobStatus.COMPLETED_WITH_ERRORS, JobStatus.PROCESSED]:
            menu.addAction("Save PDF Report", lambda: self.report_manager.save_pdf_report(job.report))
            if job.job_type != "mhl_verify":
                menu.addAction("Save MHL Manifest", lambda: self.report_manager.save_mhl_manifest(job.report))
                menu.addAction("Save CSV Log", lambda: self.report_manager.save_csv_log(job.report))
            menu.addSeparator()
        if job.status == JobStatus.COMPLETED and job.job_type != "mhl_verify":
            menu.addAction("Run Post-Processing", lambda: self.job_manager.run_post_process_for_job(job))
        if menu.actions():
            menu.exec(self.job_list.mapToGlobal(pos))
            
    def _show_ejection_dialog(self, sources):
        source_names = "\n".join([f"- {os.path.basename(p)}" for p in sources])
        reply = QMessageBox.question(self, "Eject Sources?", f"The following sources were verified successfully and can be ejected. Eject them now?\n\n{source_names}", QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
        if reply == QMessageBox.Yes:
            for path in sources: self.on_eject_requested(path)

    def show_mhl_verify_report(self, report_data):
        msg_box = QMessageBox(self); msg_box.setWindowTitle("MHL Verification Issues"); msg_box.setIcon(QMessageBox.Warning)
        summary = (f"Verification completed with {report_data['failed_count']} failed checksum(s) " f"and {report_data['missing_count']} missing file(s).")
        details = ""
        failed_files = [f for f in report_data['files'] if f['status'] == 'FAILED']
        missing_files = [f for f in report_data['files'] if f['status'] == 'Missing']
        if failed_files:
            details += "<b>Failed Checksums:</b>\n"
            for f in failed_files: details += f"• {os.path.basename(f['path'])}\n"
        if missing_files:
            details += "\n<b>Missing Files:</b>\n"
            for f in missing_files: details += f"• {os.path.basename(f['path'])}\n"
        msg_box.setText(summary); msg_box.setInformativeText("See details below. A full PDF report can also be saved.")
        text_edit = QTextEdit(); text_edit.setHtml(details); text_edit.setReadOnly(True); text_edit.setMinimumHeight(150)
        grid_layout = msg_box.layout(); grid_layout.addWidget(text_edit, grid_layout.rowCount(), 0, 1, grid_layout.columnCount())
        msg_box.exec()

    def save_session_report(self):
        if not self.job_manager.completed_jobs: QMessageBox.information(self, "No Jobs", "There are no completed jobs to report."); return
        copy_jobs = [j for j in self.job_manager.completed_jobs if j.job_type == "copy"]
        if not copy_jobs: QMessageBox.information(self, "No Copy Jobs", "Session reports can only be generated for copy jobs."); return
        consolidated_report = {'job_id': f"SESSION_{datetime.now().strftime('%Y%m%d_%H%M%S')}", 'start_time': copy_jobs[0].report['start_time'], 'end_time': copy_jobs[-1].report['end_time'], 'sources': list(set(s for j in copy_jobs for s in j.report['sources'])), 'destinations': list(set(d for j in copy_jobs for d in j.report['destinations'])), 'checksum_method': copy_jobs[0].report['checksum_method'], 'files': [f for j in copy_jobs for f in j.report['files']], 'status': 'Session Complete', 'total_size': sum(j.report['total_size'] for j in copy_jobs)}
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
        if self.project_path: self._save_project_state(); self.global_settings["last_project"] = None; self.save_settings(); self.project_path = None
        self.hide()
        dialog = ProjectManagerDialog(getattr(self, "recent_projects", []), self)
        dialog.project_selected.connect(self._load_project); dialog.new_project_requested.connect(self.new_project)
        if not dialog.exec() and not self.project_path: sys.exit()
                
    def _add_to_recent_projects(self, path):
        if not hasattr(self, "recent_projects"): self.recent_projects = []
        if path in self.recent_projects: self.recent_projects.remove(path)
        self.recent_projects.insert(0, path); self.recent_projects = self.recent_projects[:5]
        self.global_settings["last_project"] = path; self._populate_recent_menu(); self.save_settings()
        
    def _populate_recent_menu(self):
        self.recent_menu.clear()
        if hasattr(self, "recent_projects") and self.recent_projects:
            for path in self.recent_projects:
                action = QAction(os.path.basename(path), self); action.setData(path)
                action.triggered.connect(self._open_recent_project); self.recent_menu.addAction(action)
        self.recent_menu.setEnabled(bool(self.recent_menu.actions()))
        
    def _open_recent_project(self):
        if self.project_path: self._save_project_state()
        if action := self.sender():
            path = action.data()
            if os.path.exists(path): self._load_project(path)
            else: QMessageBox.warning(self, "Project Not Found", "The project path could not be found."); self.recent_projects.remove(path); self._populate_recent_menu()

    def show_mhl_verify_dialog(self):
        dialog = MHLVerifyDialog(self); dialog.add_job_requested.connect(self.on_mhl_job_add_requested); dialog.exec()

    def on_mhl_job_add_requested(self, mhl_path, target_dir):
        job = Job(job_type="mhl_verify", mhl_file=mhl_path, target_dir=target_dir)
        self.job_manager.add_job_to_queue(job)

    def save_job_template(self):
        default_name = f"{os.path.basename(self.project_path or 'Untitled')}_Template.dittemplate"
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Job Template", default_name, "DIT Templates (*.dittemplate)")
        if not file_path: return
        template_data = { "destinations": self.dest_frame.path_list.get_all_paths(), "checksum_method": self.checksum_combo.currentData(), "create_source_folder": self.create_source_folder_checkbox.isChecked(), "eject_on_completion": self.eject_checkbox.isChecked(), "skip_existing": self.skip_existing_checkbox.isChecked(), "resume_partial": self.resume_checkbox.isChecked() }
        try:
            with open(file_path, 'w') as f: json.dump(template_data, f, indent=2)
            QMessageBox.information(self, "Success", "Job template saved successfully.")
        except Exception as e: QMessageBox.critical(self, "Error", f"Could not save template: {e}")

    def load_job_template(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Load Job Template", "", "DIT Templates (*.dittemplate)")
        if not file_path: return
        try:
            with open(file_path, 'r') as f: template_data = json.load(f)
            self.source_frame.path_list.clear(); self.dest_frame.path_list.clear()
            for path in template_data.get("destinations", []): self.dest_frame.path_list.add_path(path)
            
            checksum_value = template_data.get("checksum_method", "xxHash (Fast)")
            index = self.checksum_combo.findData(checksum_value)
            if index != -1: self.checksum_combo.setCurrentIndex(index)

            self.create_source_folder_checkbox.setChecked(template_data.get("create_source_folder", True))
            self.eject_checkbox.setChecked(template_data.get("eject_on_completion", False))
            self.skip_existing_checkbox.setChecked(template_data.get("skip_existing", True))
            self.resume_checkbox.setChecked(template_data.get("resume_partial", True))
            QMessageBox.information(self, "Success", "Job template loaded. Please add your source drives.")
        except Exception as e: QMessageBox.critical(self, "Error", f"Could not load template: {e}")

    def closeEvent(self, event):
        if self.project_path: self._save_project_state()
        if self.job_manager.is_running:
            reply = QMessageBox.question(self, "Exit Confirmation", "A transfer is in progress. Are you sure you want to exit?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply == QMessageBox.Yes: self.job_manager.cancel_queue(); event.accept()
            else: event.ignore()
        else: event.accept()

if __name__ == '__main__':
    multiprocessing.freeze_support()
    if not os.path.exists(PROJECTS_BASE_DIR): os.makedirs(PROJECTS_BASE_DIR)
    app = QApplication(sys.argv)
    resources_rc.qInitResources()
    app.setStyle("Fusion")
    window = MainWindow()
    sys.exit(app.exec())