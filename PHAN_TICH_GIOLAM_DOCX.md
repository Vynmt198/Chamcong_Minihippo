# Phân tích file GIỜ-LÀM.docx và so sánh với code hiện tại

## 1. Nội dung trích từ GIỜ-LÀM.docx

| Nhóm | Ca | Giờ (theo doc) |
|------|-----|----------------|
| **SALE, PAGE** (chung sáng/chiều) | Sáng | 8h30 – 12h00 |
| | Chiều | 13h15 – 16h45 |
| **SALE** (ca tối riêng) | Tối | 18h15 – 21h30 |
| **PAGE** (ca tối riêng) | Tối | **18h30 – 22h30** |
| **Lễ tân (trừ Ánh Tuyết MH0044)** | Sáng | 8h15 – 11h45 |
| | Chiều | 13h00 – 17h00 |
| **Ánh Tuyết (MH0044)** | Sáng | 9h15 – 13h00 |
| | Chiều | 14h15 – 18h00 |
| **QL FULL** | Sáng | 9h00 – 12h00 |
| | Chiều | 13h15 – 17h15 |

---

## 2. So sánh với code hiện tại (Mã.js)

### 2.1. Default (SALE/PAGE sáng, chiều)
- **Code:** 08:30–12:00, 13:15–16:45 → **Khớp doc**, không đổi.

### 2.2. Ca tối PAGE (Team Page)
- **Code hiện tại:** 18:15–21:30, trễ từ 18:45.
- **Doc:** PAGE ca tối **18h30 – 22h30**.
- **Thay đổi:** Đổi ca tối Team Page thành **18:30–22:30**, trễ 30p = **19:00**.

### 2.3. Ca tối SALE
- Doc: SALE ca tối 18h15–21h30. Trong code hiện chỉ có TEAM = "Page", không có nhóm "SALE" riêng. Giữ nguyên logic: nếu sau này có cột SALE thì có thể thêm template 18:15–21:30.

### 2.4. Lễ tân (reception)
- **Code hiện tại:**
  - MH0043 (caDoi12): 08:15–12:15, 13:15–16:30
  - MH0044 (caDoi23): 12:15–16:15, 16:15–20:00 (2 ca chiều)
  - MH0045 (caLe): 3 ca 08:15–12:15, 12:15–16:15, 16:15–20:15

- **Theo doc:**
  - **Lễ tân trừ MH0044** (tức MH0043, MH0045): Sáng **08:15–11:45**, Chiều **13:00–17:00** (2 ca).
  - **MH0044 (Ánh Tuyết):** Sáng **09:15–13:00**, Chiều **14:15–18:00** (2 ca).

- **Thay đổi:**
  - MH0043, MH0045: 1 template chung **leTan**: ca_sang 08:15–11:45, ca_chieu 13:00–17:00.
  - MH0044: template **anhTuyet**: ca_sang 09:15–13:00, ca_chieu 14:15–18:00.

### 2.5. QL FULL (managers)
- **Code:** 09:00–12:00, 13:15–17:15 → **Khớp doc**, không đổi.

---

## 3. Tóm tắt chỉnh sửa cần làm

1. **SPECIAL_SCHEDULES.reception:** Đổi mapping và templates theo doc (leTan cho MH0043/MH0045, anhTuyet cho MH0044).
2. **computeSessionsBySchedule_:** Ca tối PAGE đổi 18:15–21:30 → **18:30–22:30**, EVENING_LATE_THRESHOLD = 19:00.
3. **QUY_DINH_GIO_LAM_VA_ROLE.md:** Cập nhật lại bảng giờ theo doc.
