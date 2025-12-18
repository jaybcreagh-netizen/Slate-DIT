# report_manager.py
import os
import csv
import platform
import subprocess
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from PySide6.QtWidgets import QFileDialog, QInputDialog, QMessageBox
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle, Flowable
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.lib.units import inch

from config import APP_NAME, APP_VERSION, FFMPEG_PATH, FFPROBE_PATH
from utils import format_bytes
from workers import PostProcessWorker, ReportWorker

class ContactSheetItem(Flowable):
    def __init__(self, image_path, filename, width, height):
        Flowable.__init__(self)
        self.image_path = image_path
        self.filename = filename
        self.width = width
        self.height = height

    def draw(self):
        if self.image_path and os.path.exists(self.image_path):
            try:
                img = Image(self.image_path, width=self.width, height=self.height - 15)
                img.hAlign = 'CENTER'
                img.drawOn(self.canv, 0, 15)
            except Exception as e:
                print(f"Error drawing image {self.image_path} in contact sheet: {e}")
                self.canv.drawString(10, self.height / 2, "Image Error")
        
        self.canv.setFont("Helvetica", 6)
        self.canv.drawCentredString(self.width / 2.0, 5, self.filename)

class ReportManager:
    def __init__(self, window):
        self.window = window
        self.report_worker = None

    # --- REWRITE: This method now ONLY handles transfer/MHL reports ---
    def save_pdf_report(self, report):
        if 'mhl_file' in report:
            self._generate_report(self._build_mhl_verify_pdf, report, "Report")
        else:
            self._generate_report(self._build_copy_pdf_interactive, report, "Report")
            
    def save_contact_sheet(self, report):
        self._generate_report(self._build_contact_sheet_pdf, report, "ContactSheet")

    def _generate_report(self, generator_func, report, report_suffix):
        default_name = f"{os.path.basename(self.window.project_path)}_{report['job_id']}_{report_suffix}.pdf"
        dialog_title = f"Save {report_suffix.replace('_', ' ')}"
        file_path, _ = QFileDialog.getSaveFileName(self.window, dialog_title, default_name, "PDF Files (*.pdf)")
        if not file_path:
            return

        self.window.show_status_message(f"Generating {report_suffix} for {report['job_id']}...", 0)
        
        self.report_worker = ReportWorker(generator_func, report, file_path)
        self.report_worker.finished.connect(self.on_report_finished)
        self.report_worker.start()
        
    def _build_copy_pdf_interactive(self, report, file_path):
        shoot_day, ok = QInputDialog.getText(self.window, "Shoot Day", "Enter Shoot Day / Date (for report):")
        if not ok:
            shoot_day = ""
        self._build_copy_pdf(report, file_path, shoot_day)

    def on_report_finished(self, success, file_path, error_message):
        self.window.clear_status_message()
        if success:
            QMessageBox.information(self.window, "Success", f"Report saved successfully to:\n{file_path}")
        else:
            QMessageBox.critical(self.window, "Report Generation Error", f"Could not generate report:\n{error_message}")
        self.report_worker = None
    
    def _build_contact_sheet_pdf(self, report, file_path):
        doc = SimpleDocTemplate(file_path, pagesize=landscape(letter),
                                leftMargin=0.5*inch, rightMargin=0.5*inch,
                                topMargin=0.5*inch, bottomMargin=0.5*inch)
        styles = getSampleStyleSheet()
        story = []
        
        prod_title = self.window.global_settings.get("production_title", os.path.basename(self.window.project_path))
        story.append(Paragraph(f"Contact Sheet: {prod_title}", styles['h1']))
        story.append(Paragraph(f"Source: {', '.join(report['sources'])}", styles['h3']))
        story.append(Spacer(1, 0.25*inch))
        
        image_files = [f for f in report['files'] if f.get('thumbnail') and os.path.exists(f.get('thumbnail'))]
        
        if not image_files:
            story.append(Paragraph("No images with thumbnails found in this job.", styles['BodyText']))
            doc.build(story)
            return

        cols = 5
        rows = 4
        
        table_width = doc.width
        cell_width = table_width / cols
        cell_height = (doc.height - 1*inch) / rows

        data = []
        row_data = []
        for f in image_files:
            item = ContactSheetItem(f['thumbnail'], os.path.basename(f['source']), cell_width, cell_height)
            row_data.append(item)
            if len(row_data) == cols:
                data.append(row_data)
                row_data = []
        
        if row_data:
            data.append(row_data)

        table = Table(data, colWidths=[cell_width]*cols, rowHeights=[cell_height]*len(data))
        table.setStyle(TableStyle([
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ]))

        story.append(table)
        doc.build(story)

    def _build_mhl_verify_pdf(self, report, file_path):
        doc = SimpleDocTemplate(file_path, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []
        story.append(Paragraph("MHL Verification Report", styles['h1']))
        story.append(Paragraph(f"Job ID: {report['job_id']}", styles['h3']))
        story.append(Spacer(1, 12))
        job_info = [
            ["Status", report['status']],
            ["MHL File", Paragraph(report['mhl_file'], styles['Code'])],
            ["Verified Against", Paragraph(report['target_dir'], styles['Code'])],
            ["Start Time", report['start_time'].strftime('%Y-%m-%d %H:%M:%S')],
            ["End Time", report['end_time'].strftime('%Y-%m-%d %H:%M:%S')]
        ]
        info_table = Table(job_info, colWidths=[100, 350])
        info_table.setStyle(TableStyle([('ALIGN', (0,0), (0,-1), 'RIGHT'), ('VALIGN', (0,0), (-1,-1), 'TOP')]))
        story.append(info_table)
        story.append(Spacer(1, 12))
        summary_style = TableStyle([('BOX', (0,0), (-1,-1), 1, colors.black), ('GRID', (0,0), (-1,-1), 0.5, colors.grey)])
        summary_data = [
            [f"Verified Files: {report['verified_count']}",
             f"Failed Checksums: {report['failed_count']}",
             f"Missing Files: {report['missing_count']}"]
        ]
        summary_table = Table(summary_data, colWidths=['33%', '33%', '33%'])
        summary_table.setStyle(summary_style)
        story.append(summary_table)
        story.append(PageBreak())
        failed_files = [f for f in report['files'] if f['status'] == 'FAILED']
        missing_files = [f for f in report['files'] if f['status'] == 'Missing']
        if failed_files:
            story.append(Paragraph("Failed Checksums", styles['h2']))
            for f in failed_files:
                story.append(Paragraph(f"<b>File:</b> {f['path']}", styles['Code']))
                story.append(Paragraph(f"<font color=red><b>FAILED</b></font> - Expected: {f['expected_hash']} ({f['hash_type']})", styles['BodyText']))
                story.append(Paragraph(f"<font color=red><b>FAILED</b></font> - Actual:   {f['actual_hash']}", styles['BodyText']))
                story.append(Spacer(1, 12))
        if missing_files:
            story.append(Paragraph("Missing Files", styles['h2']))
            for f in missing_files:
                story.append(Paragraph(f['path'], styles['Code']))
            story.append(Spacer(1, 12))
        doc.build(story)

    def _build_copy_pdf(self, report, file_path, shoot_day=""):
        doc = SimpleDocTemplate(file_path, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []
        prod_title = self.window.global_settings.get("production_title", os.path.basename(self.window.project_path))
        dit_name = self.window.global_settings.get("dit_name")
        logo_path = self.window.global_settings.get("company_logo")
        if logo_path and os.path.exists(logo_path):
            try:
                logo_img = Image(logo_path, width=100, height=50, hAlign='RIGHT')
                header_table = Table([[Paragraph(prod_title, styles['h1']), logo_img]], colWidths=['75%', '25%'])
                header_table.setStyle(TableStyle([('VALIGN', (0,0), (-1,-1), 'TOP')]))
                story.append(header_table)
            except Exception:
                story.append(Paragraph(prod_title, styles['h1']))
        else:
            story.append(Paragraph(prod_title, styles['h1']))
        header_info = []
        if dit_name:
            header_info.append(f"DIT: {dit_name}")
        if shoot_day:
            header_info.append(f"Shoot Day: {shoot_day}")
        if header_info:
            story.append(Paragraph(" &nbsp; ".join(header_info), styles['h2']))
        story.append(Spacer(1,12))
        story.append(Paragraph(f"Job ID: {report['job_id']}", styles['h3']))
        story.append(Spacer(1, 12))
        job_info = [["Status", report['status']], ["Start Time", report['start_time'].strftime('%Y-%m-%d %H:%M:%S')],
                    ["End Time", report['end_time'].strftime('%Y-%m-%d %H:%M:%S')],
                    ["Total Duration", str(report['end_time'] - report['start_time']).split('.')[0]],
                    ["Total Size", format_bytes(report['total_size'])], ["Checksum Method", report['checksum_method']],
                    ["Sources", "\n".join(report['sources'])], ["Destinations", "\n".join(report['destinations'])]]
        info_table = Table(job_info, colWidths=[100, 350])
        info_table.setStyle(TableStyle([('ALIGN', (0,0), (0,-1), 'RIGHT'), ('VALIGN', (0,0), (-1,-1), 'TOP')]))
        story.append(info_table)
        story.append(PageBreak())
        thumb_mode = self.window.global_settings.get("pdf_thumbnail_mode", "single")
        detail_level = self.window.global_settings.get("pdf_detail_level", "detailed")
        temp_thumbs = []
        try:
            for file in report['files']:
                story.append(Paragraph(f"File: {os.path.basename(file['source'])}", styles['h3']))
                if thumb_mode == "single":
                    thumb_path = file.get('thumbnail')
                    if thumb_path and os.path.exists(thumb_path):
                        try:
                            img = Image(thumb_path, width=160, height=90)
                            img.hAlign = 'LEFT'
                            story.append(img)
                            story.append(Spacer(1, 6))
                        except Exception:
                            pass
                elif thumb_mode == "filmstrip":
                    verified_dest = next((d['path'] for d in file['destinations'] if d.get('verified')), None)
                    worker = PostProcessWorker(None, self.window.project_path)
                    if verified_dest and worker._is_video_file(verified_dest):
                        filmstrip_paths = [file.get('thumbnail')] + self._generate_additional_thumbs(verified_dest, 4)
                        temp_thumbs.extend(filmstrip_paths[1:])
                        filmstrip_imgs = [Image(p, width=80, height=45) for p in filmstrip_paths if p and os.path.exists(p)]
                        if filmstrip_imgs:
                            filmstrip_table = Table([filmstrip_imgs])
                            filmstrip_table.setStyle(TableStyle([('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
                            story.append(filmstrip_table)
                            story.append(Spacer(1,6))
                file_details = [["Source Path", Paragraph(file['source'], styles['Code'])], ["Size", format_bytes(file['size'])],
                                ["Checksum", file['checksum']], ["Status", file['status']]]
                if detail_level == "detailed":
                    custom_meta = file.get('custom_metadata', {})
                    if any(custom_meta.values()):
                        file_details.append(["---", "---"])
                        if custom_meta.get('camera'):
                            file_details.append(["Camera", custom_meta['camera']])
                        if custom_meta.get('lens'):
                            file_details.append(["Lens", custom_meta['lens']])
                        if custom_meta.get('notes'):
                            file_details.append(["Notes", Paragraph(custom_meta['notes'], styles['BodyText'])])
                    meta = file.get('metadata', {})
                    if meta:
                        file_details.extend([["Format", meta.get('format', 'N/A')], ["Codec", meta.get('codec', 'N/A')],
                                             ["Resolution", meta.get('resolution', 'N/A')], ["FPS", f"{meta.get('fps', 0):.2f}"]])
                file_table = Table(file_details, colWidths=[100, 350])
                file_table.setStyle(TableStyle([('ALIGN', (0,0), (0,-1), 'RIGHT')]))
                story.append(file_table)
                dest_header = [["Destination", "Verified"]]
                dest_data = [[Paragraph(d['path'], styles['Code']), 'Yes' if d.get('verified') else 'No'] for d in file['destinations']]
                dest_table = Table(dest_header + dest_data, colWidths=[380, 70])
                dest_table.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,0), colors.grey), ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke), ('GRID', (0,0), (-1,-1), 1, colors.black)]))
                story.append(Spacer(1, 6))
                story.append(dest_table)
                story.append(Spacer(1, 24))
            doc.build(story)
        finally:
            for thumb in temp_thumbs:
                if thumb and os.path.exists(thumb):
                    try:
                        os.remove(thumb)
                    except OSError:
                        pass

    def _generate_additional_thumbs(self, video_path, count=4, thumb_size=(160,90)):
        try:
            creationflags = subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0
            cmd = [FFPROBE_PATH, "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", video_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, creationflags=creationflags)
            duration = float(result.stdout)
            thumb_paths = []
            for i in range(count):
                percent = (i + 2) / (count + 2)
                seek_time = duration * percent
                temp_dir = os.path.join(self.window.project_path, ".dit_project", "thumbnails")
                thumb_name = f"temp_{os.path.basename(video_path)}_{i}.jpg"
                thumb_path = os.path.join(temp_dir, thumb_name)
                cmd_ffmpeg = [ FFMPEG_PATH, '-y', '-ss', str(seek_time), '-i', video_path, '-vf', f'scale={thumb_size[0]}:{thumb_size[1]}:force_original_aspect_ratio=decrease,pad={thumb_size[0]}:{thumb_size[1]}:(ow-iw)/2:(oh-ih)/2', '-vframes', '1', thumb_path ]
                subprocess.run(cmd_ffmpeg, check=True, capture_output=True, creationflags=creationflags)
                if os.path.exists(thumb_path):
                    thumb_paths.append(thumb_path)
            return thumb_paths
        except (subprocess.CalledProcessError, FileNotFoundError, ValueError) as e:
            print(f"Could not generate filmstrip for {video_path}: {e}")
            return []

    def save_mhl_manifest(self, report):
        default_name = f"{os.path.basename(self.window.project_path)}_{report['job_id']}.mhl"
        file_path, _ = QFileDialog.getSaveFileName(self.window, "Save MHL Manifest", default_name, "MHL Files (*.mhl)")
        if not file_path:
            return
        root = Element('hashlist', version='1.1')
        creatorinfo = SubElement(root, 'creatorinfo')
        SubElement(creatorinfo, 'hostname').text = platform.node()
        SubElement(creatorinfo, 'username').text = os.getlogin()
        SubElement(creatorinfo, 'tool').text = f"{APP_NAME} {APP_VERSION}"
        SubElement(creatorinfo, 'startdate').text = report['start_time'].isoformat()
        SubElement(creatorinfo, 'finishdate').text = report['end_time'].isoformat()
        for file in report['files']:
            if file['status'] != 'Verified':
                continue
            hash_tag = 'xxhash64' if 'xxHash' in report['checksum_method'] else 'md5'
            for dest in file['destinations']:
                if dest['verified']:
                    relative_path = os.path.relpath(dest['path'], os.path.dirname(file_path))
                    hash_element = SubElement(root, 'hash')
                    SubElement(hash_element, 'file').text = relative_path
                    SubElement(hash_element, 'size').text = str(file['size'])
                    SubElement(hash_element, hash_tag).text = file['checksum']
        xml_string = tostring(root, 'utf-8')
        pretty_xml = minidom.parseString(xml_string).toprettyxml(indent="  ")
        with open(file_path, "w") as f:
            f.write(pretty_xml)
        QMessageBox.information(self.window, "Success", f"MHL manifest saved to {file_path}")
        
    def save_csv_log(self, report):
        default_name = f"{os.path.basename(self.window.project_path)}_{report['job_id']}_Log.csv"
        file_path, _ = QFileDialog.getSaveFileName(self.window, "Save CSV Log", default_name, "CSV Files (*.csv)")
        if not file_path:
            return
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Source File', 'Destination File', 'Size (Bytes)', 'Checksum', 'Checksum Method', 'Status'])
            for file in report['files']:
                for dest in file['destinations']:
                    if dest.get('verified') is True:
                        status = "Verified"
                    else:
                        status = dest.get('status', 'Verification FAILED') 
                    writer.writerow([file['source'], dest['path'], file['size'], file['checksum'], report['checksum_method'], status])
        QMessageBox.information(self.window, "Success", f"CSV log saved to {file_path}")