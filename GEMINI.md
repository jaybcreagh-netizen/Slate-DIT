# GEMINI.md

## Project Overview

This project is a desktop application named "Slate" built with Python and the PySide6 (Qt) GUI framework. It appears to be a professional tool for managing file transfers, particularly for media production workflows.

The application provides a graphical user interface for:

*   **Project Management:** Creating and managing projects, each with its own set of sources, destinations, and settings.
*   **File Transfers:** Copying files from source to destination with features like:
    *   Checksum verification (xxHash, MD5)
    *   Job queuing and concurrent transfers
    *   Skipping existing files
    *   Resuming partial transfers
    *   Ejecting drives on completion
*   **Reporting:** Generating transfer reports in PDF format, including contact sheets and MHL manifests.
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

There is also evidence of PyInstaller usage (`hook-PySide6.py`), which suggests the project can be bundled into a standalone executable.

## Development Conventions

*   **GUI Framework:** The project uses PySide6 for its graphical user interface.
*   **Styling:** The application is styled using a custom QSS stylesheet (`style.qss`).
*   **Concurrency:** The application uses a `JobManager` to handle concurrent file transfers using a worker-based architecture (`TransferWorker`, `PostProcessWorker`, etc.).
*   **Project Structure:** The code is organized into several modules:
    *   `main.py`: The main application entry point and main window.
    *   `config.py`: Application configuration.
    *   `job_manager.py`: Core logic for managing transfer jobs.
    *   `workers.py`: Worker classes for performing background tasks.
    *   `ui_components.py`: Reusable UI components.
    *   `utils.py`: Utility functions.
    *   `report_manager.py`: Logic for generating reports.
    *   `models.py`: Data models.
*   **Resource Management:** The application uses `.qrc` files (`resources.qrc`, `sounds.qrc`) to bundle resources like icons and sounds into the application.
