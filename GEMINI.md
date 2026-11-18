# GEMINI.md

## Project Overview

This project is a desktop application named "Slate" built with Python and the PySide6 (Qt) GUI framework. It appears to be a professional tool for managing file transfers, particularly for media production workflows.

The application provides a graphical user interface for:

*   **Project Management:** Creating and managing projects, each with its own set of sources, destinations, and settings.
*   **File Transfers:** Copying files from source to destination with features like:
    *   Checksum verification (xxHash, MD5): each copy is read back from the drive and compared with the source
    *   Job queuing and concurrent transfers
    *   Never overwriting: a file already at the destination is checked against the source, not replaced
    *   No half-written files: a file is named `<name>.slate-partial` until it passes its check
    *   Ejecting drives on completion (only offered when every file was verified)
*   **Proof of copy:** An ASC MHL manifest (`ascmhl/` folder) is written automatically at the top of each copied card on every destination, using the ASC reference library.
*   **Reporting:** Generating transfer reports in PDF format, including contact sheets. A legacy MHL (v1.1) export is also available.
*   **Post-Processing:** Running post-transfer tasks, likely involving `ffmpeg` and `ffprobe` for media file analysis and thumbnail generation.
*   **Template Management:** Saving and loading job templates to streamline repetitive tasks.

The application is designed to be cross-platform, with specific handling for macOS.

## Building and Running

The project uses a `requirements.txt` file to manage Python dependencies. To run the application, you would typically perform the following steps:

1.  **Create a virtual environment:**
    ```bash
    python3 -m venv venv
    ```

2.  **Activate the virtual environment:**
    ```bash
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run the application:**
    ```bash
    python main.py
    ```

5.  **Run the tests:**
    ```bash
    python -m unittest discover -s tests
    ```
    `tests/test_faults.py` damages copies on purpose (flipped bit, full drive, unplugged drive, cancel) and checks the job reports it. `tests/test_manifest.py` checks the manifests with the ASC reference tool.

6.  **Time a copy on real hardware:**
    ```bash
    python bench_offload.py /Volumes/CARD /Volumes/DriveA /Volumes/DriveB
    ```

There is also evidence of PyInstaller usage (`hook-PySide6.py`), which suggests the project can be bundled into a standalone executable.

## Development Conventions

*   **GUI Framework:** The project uses PySide6 for its graphical user interface.
*   **Styling:** The application is styled using a custom QSS stylesheet (`style.qss`).
*   **Concurrency:** The application uses a `JobManager` to handle concurrent file transfers using a worker-based architecture (`TransferWorker`, `PostProcessWorker`, etc.).
*   **Project Structure:** The code is organized into several modules:
    *   `main.py`: The main application entry point and main window.
    *   `config.py`: Application configuration.
    *   `job_manager.py`: Core logic for managing transfer jobs.
    *   `copy_engine.py`: Copies one file to every destination and verifies it. No Qt, so it can be tested and timed alone.
    *   `manifest.py`: Writes the ASC MHL manifest beside each copy.
    *   `workers.py`: Worker classes for performing background tasks.
    *   `ui_components.py`: Reusable UI components.
    *   `utils.py`: Utility functions.
    *   `report_manager.py`: Logic for generating reports.
    *   `models.py`: Data models.
*   **Resource Management:** The application uses `.qrc` files (`resources.qrc`, `sounds.qrc`) to bundle resources like icons and sounds into the application.
