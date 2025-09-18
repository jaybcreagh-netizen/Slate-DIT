# ui_components.py
import os
import platform
import subprocess

import psutil
import qtawesome as qta
from PySide6.QtCore import Qt, Signal, QSize
from PySide6.QtGui import QMouseEvent, QFont, QAction
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFrame,
    QListWidget, QListWidgetItem, QFileDialog, QDialog, QLineEdit,
    QMenu, QMessageBox, QTextEdit, QFormLayout, QGroupBox,
    QInputDialog, QTabWidget, QComboBox, QProgressBar, QSpinBox, QCheckBox
)

from utils import get_icon_for_path, format_bytes, resolve_path_template

class JobListItem(QWidget):
    """ Simplified Job List Item to show only job identity and status. """
    def __init__(self, job_data, parent=None):
        super().__init__(parent)
        self.job_id = job_data['id']
        
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)
        main_layout.setSpacing(10)

        self.status_icon = QLabel()
        self.job_label = QLabel()
        
        main_layout.addWidget(self.status_icon)
        main_layout.addWidget(self.job_label)
        main_layout.addStretch()

        self.update_status(job_data)

    def update_status(self, job_data):
        status = job_data['status']
        
        if job_data.get("job_type") == "mhl_verify":
            job_name = os.path.basename(job_data['mhl_file'])
            item_text = f"<b>MHL Verify:</b> {job_name}"
        else:
            source_names = ", ".join([os.path.basename(p) for p in job_data.get('sources', [])])
            dest_names = ", ".join([os.path.basename(p) for p in job_data.get('destinations', [])])
            item_text = f"<b>{source_names}</b> > {dest_names}"
        self.job_label.setText(item_text)

        colors = {"Processed": "#33FF99", "Post-processing": "purple", "Completed": "green", "Running": "cyan", "Cancelled": "orange", "Queued": "gray"}
        icon_color = next((colors[s] for s in colors if s in status), "red")
        icons = {"Processed": "fa5s.check-double", "Post-processing": "fa5s.film", "Completed": "fa5s.check-circle", "Running": "fa5s.cogs", "Cancelled": "fa5s.ban", "Queued": "fa5s.clock"}
        icon_name = next((icons[s] for s in icons if s in status), "fa5s.exclamation-circle")
        self.status_icon.setPixmap(qta.icon(icon_name, color=icon_color).pixmap(QSize(16, 16)))

class MHLVerifyDialog(QDialog):
    add_job_requested = Signal(str, str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Verify from MHL File")
        self.setMinimumWidth(500)
        self.setModal(True)

        layout = QFormLayout(self)
        layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapAllRows)

        self.mhl_path_edit = QLineEdit()
        self.mhl_path_edit.setPlaceholderText("Select the .mhl manifest file")
        browse_mhl_btn = QPushButton("Browse...")
        browse_mhl_btn.clicked.connect(self.browse_mhl)
        mhl_layout = QHBoxLayout()
        mhl_layout.addWidget(self.mhl_path_edit)
        mhl_layout.addWidget(browse_mhl_btn)
        layout.addRow(QLabel("<b>MHL Manifest File:</b>"), mhl_layout)

        self.target_dir_edit = QLineEdit()
        self.target_dir_edit.setPlaceholderText("Select the root directory to verify")
        browse_dir_btn = QPushButton("Browse...")
        browse_dir_btn.clicked.connect(self.browse_dir)
        dir_layout = QHBoxLayout()
        dir_layout.addWidget(self.target_dir_edit)
        dir_layout.addWidget(browse_dir_btn)
        layout.addRow(QLabel("<b>Target Directory:</b>"), dir_layout)

        self.add_button = QPushButton(qta.icon("fa5s.plus-circle", color="white"), " Add Job to Queue")
        self.add_button.clicked.connect(self.add_job)
        self.add_button.setEnabled(False)
        layout.addRow("", self.add_button)

        self.mhl_path_edit.textChanged.connect(self.check_inputs)
        self.target_dir_edit.textChanged.connect(self.check_inputs)

    def browse_mhl(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select MHL File", "", "MHL Files (*.mhl)")
        if path:
            self.mhl_path_edit.setText(path)

    def browse_dir(self):
        path = QFileDialog.getExistingDirectory(self, "Select Target Directory")
        if path:
            self.target_dir_edit.setText(path)

    def check_inputs(self):
        mhl_ok = os.path.isfile(self.mhl_path_edit.text())
        dir_ok = os.path.isdir(self.target_dir_edit.text())
        self.add_button.setEnabled(mhl_ok and dir_ok)

    def add_job(self):
        self.add_job_requested.emit(self.mhl_path_edit.text(), self.target_dir_edit.text())
        self.accept()

class ProjectManagerDialog(QDialog):
    project_selected = Signal(str)
    new_project_requested = Signal()

    def __init__(self, recent_projects, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Project Manager")
        self.setMinimumWidth(400)
        self.setModal(True)
        
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("<h2>Select a Project</h2>"))
        
        self.project_list = QListWidget()
        self.project_list.addItems([os.path.basename(p) for p in recent_projects])
        self.project_list.itemDoubleClicked.connect(self.open_selected)
        self.recent_projects_paths = recent_projects
        layout.addWidget(self.project_list)
        
        buttons_layout = QHBoxLayout()
        new_button = QPushButton("Create New Project"); new_button.clicked.connect(self.new_project_requested)
        open_other_button = QPushButton("Open Other..."); open_other_button.clicked.connect(self.open_other)
        open_selected_button = QPushButton("Open Selected"); open_selected_button.clicked.connect(self.open_selected)
        open_selected_button.setDefault(True)
        quit_button = QPushButton("Quit"); quit_button.clicked.connect(self.reject)

        buttons_layout.addWidget(new_button); buttons_layout.addWidget(open_other_button)
        buttons_layout.addStretch()
        buttons_layout.addWidget(open_selected_button); buttons_layout.addWidget(quit_button)
        layout.addLayout(buttons_layout)

    def open_selected(self):
        selected_item = self.project_list.currentItem()
        if selected_item:
            index = self.project_list.currentRow()
            path = self.recent_projects_paths[index]
            self.project_selected.emit(path)
            self.accept()

    def open_other(self):
        from config import PROJECTS_BASE_DIR
        path = QFileDialog.getExistingDirectory(self, "Select Project Folder", dir=PROJECTS_BASE_DIR)
        if path and os.path.isdir(os.path.join(path, ".dit_project")):
            self.project_selected.emit(path)
            self.accept()
        elif path:
            QMessageBox.warning(self, "Invalid Project", "The selected folder is not a valid project.")

class SettingsDialog(QDialog):
    def __init__(self, global_settings, project_naming_preset, project_loaded, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setMinimumWidth(600)
        
        self.global_settings = global_settings.copy()
        self.naming_preset = project_naming_preset.copy()

        main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        self.performance_tab = QWidget()
        self.pdf_tab = QWidget()
        self.naming_tab = QWidget()
        self.tabs.addTab(self.performance_tab, "Performance")
        self.tabs.addTab(self.pdf_tab, "PDF Reports")
        self.tabs.addTab(self.naming_tab, "Naming Preset")
        
        self._setup_performance_tab()
        self._setup_pdf_tab()
        self._setup_naming_tab()
        
        self.naming_tab.setEnabled(project_loaded)
        if not project_loaded: self.tabs.setTabToolTip(2, "A project must be open to configure naming presets.")

        button_layout = QHBoxLayout()
        save_button = QPushButton("Save"); save_button.clicked.connect(self.accept)
        cancel_button = QPushButton("Cancel"); cancel_button.clicked.connect(self.reject)
        button_layout.addStretch()
        button_layout.addWidget(cancel_button); button_layout.addWidget(save_button)
        main_layout.addLayout(button_layout)
        
    def _setup_performance_tab(self):
        layout = QVBoxLayout(self.performance_tab)
        
        concurrency_group = QGroupBox("Concurrency")
        form_layout_c = QFormLayout(concurrency_group)
        self.concurrent_jobs_spinbox = QSpinBox()
        self.concurrent_jobs_spinbox.setMinimum(1)
        self.concurrent_jobs_spinbox.setMaximum(os.cpu_count() or 1)
        self.concurrent_jobs_spinbox.setValue(self.global_settings.get("concurrent_jobs", 1))
        form_layout_c.addRow("Max Concurrent Jobs:", self.concurrent_jobs_spinbox)
        layout.addWidget(concurrency_group)
        
        transfer_group = QGroupBox("Transfer & Verification")
        form_layout_t = QFormLayout(transfer_group)
        self.verification_mode_combo = QComboBox()
        self.verification_mode_combo.addItems(["Full (Hash Verification)", "File Size Check Only", "Copy Only (Unverified)"])
        self.defer_post_process_checkbox = QCheckBox("Defer post-processing")
        self.defer_post_process_checkbox.setToolTip("Run heavy tasks like thumbnail generation manually later.")
        
        form_layout_t.addRow("Verification Mode:", self.verification_mode_combo)
        form_layout_t.addRow(self.defer_post_process_checkbox)
        layout.addWidget(transfer_group)

        # Load current settings
        verify_map = {"full": 0, "size": 1, "none": 2}
        self.verification_mode_combo.setCurrentIndex(verify_map.get(self.global_settings.get("verification_mode", "full")))
        self.defer_post_process_checkbox.setChecked(self.global_settings.get("defer_post_process", False))
        
        layout.addStretch()
    
    def _setup_pdf_tab(self):
        layout = QVBoxLayout(self.pdf_tab)
        
        branding_group = QGroupBox("Report Branding")
        form_layout = QFormLayout(branding_group)
        self.prod_title_input = QLineEdit(self.global_settings.get("production_title", ""))
        self.dit_name_input = QLineEdit(self.global_settings.get("dit_name", ""))
        form_layout.addRow("Production Title:", self.prod_title_input)
        form_layout.addRow("DIT Name:", self.dit_name_input)
        logo_layout = QHBoxLayout()
        self.logo_path_label = QLabel(os.path.basename(self.global_settings.get("company_logo", "No logo selected")))
        self.logo_path_label.setStyleSheet("color: #999;")
        select_logo_button = QPushButton("Select Logo..."); select_logo_button.clicked.connect(self.select_logo)
        logo_layout.addWidget(self.logo_path_label); logo_layout.addStretch(); logo_layout.addWidget(select_logo_button)
        form_layout.addRow("Company Logo:", logo_layout)
        layout.addWidget(branding_group)

        layout_group = QGroupBox("Report Layout")
        layout_form = QFormLayout(layout_group)
        self.thumb_mode_combo = QComboBox()
        self.thumb_mode_combo.addItems(["Single Thumbnail", "Filmstrip (5)", "No Thumbnails"])
        self.detail_level_combo = QComboBox()
        self.detail_level_combo.addItems(["Detailed", "Simple"])
        layout_form.addRow("Thumbnail Mode:", self.thumb_mode_combo)
        layout_form.addRow("Detail Level:", self.detail_level_combo)
        layout.addWidget(layout_group)
        
        thumb_map = {"single": 0, "filmstrip": 1, "none": 2}
        self.thumb_mode_combo.setCurrentIndex(thumb_map.get(self.global_settings.get("pdf_thumbnail_mode", "single")))
        detail_map = {"detailed": 0, "simple": 1}
        self.detail_level_combo.setCurrentIndex(detail_map.get(self.global_settings.get("pdf_detail_level", "detailed")))
        
        layout.addStretch()
    
    def _setup_naming_tab(self):
        main_layout = QVBoxLayout(self.naming_tab)
        token_group = QGroupBox("User-defined Tokens")
        form_layout = QFormLayout(token_group)
        self.project_name_input = QLineEdit()
        self.camera_id_input = QLineEdit()
        form_layout.addRow("Project Name:", self.project_name_input)
        form_layout.addRow("Camera ID:", self.camera_id_input)
        main_layout.addWidget(token_group)
        
        template_group = QGroupBox("Folder Template")
        template_layout = QVBoxLayout(template_group)
        self.template_input = QLineEdit()
        self.template_input.setPlaceholderText("e.g., {date_yyyymmdd}/{project_name}/{camera_id}_{card_num}")
        template_layout.addWidget(self.template_input)
        available_tokens = "<b>Available Tokens:</b><br>" \
                           "<code>{date_yyyy-mm-dd}</code>, <code>{date_yyyymmdd}</code>, <code>{date_yy-mm-dd}</code><br>" \
                           "<code>{project_name}</code>, <code>{camera_id}</code><br>" \
                           "<code>{card_num}</code> (auto-increments), <code>{source_name}</code>"
        tokens_label = QLabel(available_tokens); tokens_label.setWordWrap(True)
        template_layout.addWidget(tokens_label)
        main_layout.addWidget(template_group)
        
        preview_group = QGroupBox("Live Preview")
        preview_layout = QVBoxLayout(preview_group)
        self.preview_label = QLabel("<i>Preview will appear here...</i>")
        self.preview_label.setStyleSheet("background-color: #1e1e1e; padding: 5px; border-radius: 3px;")
        self.preview_label.setWordWrap(True)
        preview_layout.addWidget(self.preview_label)
        main_layout.addWidget(preview_group)
        main_layout.addStretch()

        self.project_name_input.textChanged.connect(self.update_naming_preview)
        self.camera_id_input.textChanged.connect(self.update_naming_preview)
        self.template_input.textChanged.connect(self.update_naming_preview)

        self.project_name_input.setText(self.naming_preset.get("project_name", ""))
        self.camera_id_input.setText(self.naming_preset.get("camera_id", ""))
        self.template_input.setText(self.naming_preset.get("template", ""))
        self.update_naming_preview()

    def select_logo(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select Logo Image", "", "Image Files (*.png *.jpg *.jpeg)")
        if path:
            self.global_settings["company_logo"] = path
            self.logo_path_label.setText(os.path.basename(path))

    def update_naming_preview(self):
        preview_path = resolve_path_template(
            template=self.template_input.text(),
            user_tokens=self._get_naming_data(),
            card_num=1,
            source_name="A001C002"
        )
        self.preview_label.setText(f"<i>Example Path:</i><br><b>.../Destination/{preview_path}</b>")
        
    def _get_naming_data(self):
        return {
            "project_name": self.project_name_input.text(), 
            "camera_id": self.camera_id_input.text(), 
            "template": self.template_input.text()
        }

    def get_settings(self):
        # Performance
        self.global_settings["concurrent_jobs"] = self.concurrent_jobs_spinbox.value()
        verify_map = {0: "full", 1: "size", 2: "none"}
        self.global_settings["verification_mode"] = verify_map.get(self.verification_mode_combo.currentIndex())
        self.global_settings["defer_post_process"] = self.defer_post_process_checkbox.isChecked()
        
        # PDF Branding
        self.global_settings["production_title"] = self.prod_title_input.text()
        self.global_settings["dit_name"] = self.dit_name_input.text()

        # PDF Layout
        thumb_map = {0: "single", 1: "filmstrip", 2: "none"}
        self.global_settings["pdf_thumbnail_mode"] = thumb_map.get(self.thumb_mode_combo.currentIndex())
        detail_map = {0: "detailed", 1: "simple"}
        self.global_settings["pdf_detail_level"] = detail_map.get(self.detail_level_combo.currentIndex())
        
        return {
            "global": self.global_settings,
            "naming_preset": self._get_naming_data()
        }

class MetadataDialog(QDialog):
    def __init__(self, existing_data=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Source Metadata")
        self.setMinimumWidth(350)
        layout = QFormLayout(self)
        self.camera_input = QLineEdit(); self.lens_input = QLineEdit()
        self.notes_input = QTextEdit(); self.notes_input.setFixedHeight(80)
        layout.addRow("Camera:", self.camera_input); layout.addRow("Lens:", self.lens_input)
        layout.addRow("Notes/Shot Type:", self.notes_input)
        if existing_data:
            self.camera_input.setText(existing_data.get("camera", ""))
            self.lens_input.setText(existing_data.get("lens", ""))
            self.notes_input.setPlainText(existing_data.get("notes", ""))
        button_box = QPushButton("Save Metadata"); button_box.clicked.connect(self.accept)
        layout.addRow(button_box)
    def get_data(self):
        return {"camera": self.camera_input.text(), "lens": self.lens_input.text(), "notes": self.notes_input.toPlainText()}

class PathListItem(QWidget):
    remove_clicked = Signal(str)
    def __init__(self, path, parent=None):
        super().__init__(parent)
        self.path = path
        layout = QHBoxLayout(self); layout.setContentsMargins(5, 5, 5, 5)
        self.icon_label = QLabel(); self.icon_label.setPixmap(get_icon_for_path(path).pixmap(QSize(24, 24)))
        text_layout = QVBoxLayout(); text_layout.setSpacing(0)
        self.name_label = QLabel(f"<b>{os.path.basename(path) or path}</b>"); self.name_label.setStyleSheet("color: white;")
        self.path_label = QLabel(os.path.dirname(path));
        font = self.path_label.font(); font.setPointSize(font.pointSize() - 2); self.path_label.setFont(font)
        self.path_label.setStyleSheet("color: #999;")
        text_layout.addWidget(self.name_label); text_layout.addWidget(self.path_label)
        if os.path.ismount(path):
            try:
                usage = psutil.disk_usage(path)
                space_info = f"{format_bytes(usage.free)} free of {format_bytes(usage.total)}"
                self.space_label = QLabel(space_info)
                font = self.space_label.font(); font.setPointSize(font.pointSize() - 3); self.space_label.setFont(font)
                self.space_label.setStyleSheet("color: #888;")
                text_layout.addWidget(self.space_label)
            except Exception as e:
                print(f"Could not get disk usage for {path}: {e}")
        self.remove_button = QPushButton(qta.icon("fa5s.times", color="gray"), "")
        self.remove_button.setFlat(True); self.remove_button.setFixedSize(24, 24)
        self.remove_button.clicked.connect(lambda: self.remove_clicked.emit(self.path))
        layout.addWidget(self.icon_label); layout.addLayout(text_layout); layout.addStretch(); layout.addWidget(self.remove_button)

class PathListWidget(QListWidget):
    metadata_requested = Signal(str)
    eject_requested = Signal(str)
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)
    def add_path(self, path):
        if self.path_exists(path): return
        item = QListWidgetItem(self)
        widget = PathListItem(path)
        widget.remove_clicked.connect(self.remove_path)
        item.setSizeHint(widget.sizeHint())
        self.addItem(item); self.setItemWidget(item, widget)
    def remove_path(self, path_to_remove):
        for i in range(self.count()):
            item = self.item(i)
            widget = self.itemWidget(item)
            if widget and widget.path == path_to_remove:
                self.takeItem(i); break
    def path_exists(self, path_to_check): return path_to_check in self.get_all_paths()
    def get_all_paths(self): return [self.itemWidget(self.item(i)).path for i in range(self.count()) if self.itemWidget(self.item(i))]
    def show_context_menu(self, pos):
        item = self.itemAt(pos)
        if not item: return
        widget = self.itemWidget(item)
        if not widget: return
        path = widget.path
        main_window = self.window()
        is_transfer_running = main_window.job_manager.is_running if main_window else False
        menu = QMenu(self)
        if os.path.ismount(path):
            eject_action = QAction(qta.icon("fa5s.eject", color="white"), "Eject Drive", self)
            eject_action.triggered.connect(lambda: self.eject_requested.emit(path))
            eject_action.setEnabled(not is_transfer_running)
            if is_transfer_running:
                eject_action.setToolTip("Cannot eject while a transfer is in progress.")
            menu.addAction(eject_action)
            menu.addSeparator()
        open_action_text = "Open in Explorer" if platform.system() == "Windows" else "Open in Finder"
        open_action = QAction(open_action_text, self); open_action.triggered.connect(lambda: self.open_in_explorer(path))
        menu.addAction(open_action)
        metadata_action = QAction("Add/Edit Metadata...", self)
        metadata_action.triggered.connect(lambda: self.metadata_requested.emit(path))
        menu.addAction(metadata_action)
        remove_action = QAction("Remove From List", self); remove_action.triggered.connect(lambda: self.remove_path(path))
        menu.addAction(remove_action)
        menu.exec(self.mapToGlobal(pos))
    def open_in_explorer(self, path):
        try:
            if platform.system() == "Windows": os.startfile(path)
            elif platform.system() == "Darwin": subprocess.run(["open", path])
            else: subprocess.run(["xdg-open", path])
        except Exception as e:
            print(f"Error opening path {path}: {e}")

class DropFrame(QFrame):
    def __init__(self, title, parent=None):
        super().__init__(parent)
        self.setObjectName("DropFrame")
        self.setAcceptDrops(True)
        self.normal_style = "#DropFrame { border: 2px dashed #333; border-radius: 10px; }"
        self.hover_style = "#DropFrame { border: 2px solid #007acc; border-radius: 10px; background-color: #2a2a2a; }"
        self.setStyleSheet(self.normal_style)
        main_layout = QVBoxLayout(self)
        title_layout = QHBoxLayout(); title_layout.setContentsMargins(10, 10, 10, 5)
        self.title_label = QLabel(title); self.title_label.setFont(QFont("Arial", 12, QFont.Bold))
        title_layout.addWidget(self.title_label); title_layout.addStretch()
        add_button = QPushButton(qta.icon("fa5s.plus", color="white"), ""); add_button.setFixedSize(28, 28)
        add_button.setToolTip(f"Add {title.lower()}"); title_layout.addWidget(add_button)
        self.path_list = PathListWidget()
        main_layout.addLayout(title_layout); main_layout.addWidget(self.path_list)
        add_button.clicked.connect(self._on_add_clicked)
    def _on_add_clicked(self):
        path = QFileDialog.getExistingDirectory(self, f"Select a {self.title_label.text().lower()}")
        if path:
            self.path_list.add_path(path)
    def mouseDoubleClickEvent(self, event: QMouseEvent):
        self._on_add_clicked()
        super().mouseDoubleClickEvent(event)
    def dragEnterEvent(self, event: QMouseEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
            self.setStyleSheet(self.hover_style)
        else: event.ignore()
    def dragLeaveEvent(self, event: QMouseEvent):
        self.setStyleSheet(self.normal_style)
        event.accept()
    def dropEvent(self, event: QMouseEvent):
        self.setStyleSheet(self.normal_style)
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                path = url.toLocalFile()
                if os.path.isdir(path):
                    self.path_list.add_path(path)
            event.acceptProposedAction()
        else: event.ignore()