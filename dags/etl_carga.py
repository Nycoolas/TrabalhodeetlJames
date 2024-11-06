import zipfile
import os

zip_folder = "/Applications/trabalhodeetl20242/data"


def load_data_from_zip(zip_folder, extract_folder):
    os.makedirs(extract_folder, exist_ok=True)
    zip_files = [os.path.join(zip_folder, file) for file in os.listdir(zip_folder) if file.endswith(".ZIP")]
    
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        
        extracted_files = os.listdir(extract_folder)
        for file_name in extracted_files:
            file_path = os.path.join(extract_folder, file_name)
            with open(file_path, 'r') as f:
                data = f.read()
                print(f"Processando o arquivo {file_name}")

            os.remove(file_path)
