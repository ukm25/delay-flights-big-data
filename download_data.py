import os
import shutil
from huggingface_hub import hf_hub_download

def download_and_save():
    print("Bắt đầu tải dữ liệu trực tiếp từ Hugging Face...")
    print("File này nặng khoảng 3.6GB, vui lòng đảm bảo mạng ổn định và chờ vài phút.")
    
    repo_id = "Algo0o/flight_delay"
    filename = "data_cleaned/Airline.csv"
    
    try:
        # Tải file trực tiếp
        file_path = hf_hub_download(repo_id=repo_id, filename=filename, repo_type="dataset")
        
        # Lấy đường dẫn của folder hiện tại (FinalBigData)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        dest_path = os.path.join(current_dir, "flight_delay_18M.csv")
        
        print(f"Đã tải xong vào cache! Đang copy file ra thư mục hiện tại: {dest_path}")
        shutil.copy2(file_path, dest_path)
        
        print("✅ HOÀN TẤT! File dữ liệu gốc 18.2 triệu dòng (dạng CSV) đã được lưu.")
        print(f"📌 Đường dẫn: {dest_path}")
        
    except Exception as e:
        print(f"❌ Có lỗi xảy ra trong quá trình tải: {e}")

if __name__ == "__main__":
    download_and_save()
