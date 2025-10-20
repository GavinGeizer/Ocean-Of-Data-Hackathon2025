import os
import csv
import sys

gbif_dir = 'gbif_downloads'

def print_progress_bar(current, total, prefix='', length=40):
    percent = current / total if total else 0
    filled = int(length * percent)
    bar = '█' * filled + '-' * (length - filled)
    print(f'\r{prefix} |{bar}| {current}/{total}', end='')
    if current == total:
        print()

# Get all subdirectories
subdirs = [os.path.join(gbif_dir, d) for d in os.listdir(gbif_dir) if os.path.isdir(os.path.join(gbif_dir, d))]
num_dirs = len(subdirs)

for dir_idx, subdir in enumerate(subdirs, 1):
    print_progress_bar(dir_idx, num_dirs, prefix='Directory Progress')
    files = [f for f in os.listdir(subdir) if f.endswith('.txt')]
    num_files = len(files)
    for file_idx, file in enumerate(files, 1):
        print_progress_bar(file_idx, num_files, prefix=f'  File Progress ({os.path.basename(subdir)})')
        txt_path = os.path.join(subdir, file)
        csv_path = os.path.join(subdir, file[:-4] + '.csv')
        with open(txt_path, 'r', encoding='utf-8') as tsvfile, open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(tsvfile, delimiter='\t')
            writer = csv.writer(csvfile)
            for row in reader:
                writer.writerow(row)