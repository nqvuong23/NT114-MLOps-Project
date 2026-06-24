import pandas as pd

# 1. Đường dẫn tới file parquet của bạn
file_path = "du_lieu_cua_ban.parquet"

# 2. Đọc file parquet vào DataFrame
df = pd.read_parquet(file_path)

# 3. Cấu hình Pandas để không bị ẩn (bằng dấu ...) khi file có nhiều cột/dòng
pd.set_option('display.max_columns', None)  # Hiển thị toàn bộ các cột
pd.set_option('display.max_rows', None)     # Hiển thị toàn bộ các dòng (cẩn thận nếu file quá lớn)
pd.set_option('display.width', 1000)        # Tăng độ rộng hiển thị để tránh bị xuống dòng vô lý

# 4. Cách 1: In ra dạng bảng đẹp mắt bằng `tabulate` (Khuyên dùng để dễ nhìn)
# print("--- HIỂN THỊ DẠNG BẢNG ĐẸP (GRID) ---")
# print(df.to_markdown(index=False, tablefmt="grid"))

# 5. Cách 2: Nếu bạn muốn soi chi tiết TỪNG DÒNG theo dạng JSON/Dictionary (Khi có quá nhiều cột)
print("\n--- HIỂN THỊ CHI TIẾT TỪNG DÒNG ---")
for index, row in df.iterrows():
    print(f"\n[Dòng {index + 1}]:")
    print(row.to_dict())