# Quy định giờ làm, role và xử lý liên quan - Mã.js

Tài liệu này là bản regenerate theo quy định mới, dùng để đối chiếu nhanh giữa nghiệp vụ và `Mã.js`.

---

## 1) Cấu hình chung (cfg)

Các luồng chính (`highlightProblematicCells`, `applyAttendance`, ONL/OFF ngoài) dùng cấu hình nền:

| Trường | Mặc định | Ý nghĩa |
|---|---|---|
| `morningStart` | `08:30` | Giờ bắt đầu ca sáng mặc định |
| `afternoonStart` | `13:15` | Giờ bắt đầu ca chiều mặc định |
| `cutoff` | `12:00` | Ranh giới tách sáng/chiều |
| `lateThreshold` | `30` | Ngưỡng trễ để quy đổi trọng số lỗi |
| `maxTimesThreshold` | `4` | Số mốc tối đa/ngày cho luồng thường (Page tối đa 6) |

---

## 2) Lịch làm theo nhóm (SPECIAL_SCHEDULES)

### 2.1 Default (nhân sự thường)
- Sáng: `08:30-12:00`
- Chiều: `13:15-16:45`

### 2.2 Managers (theo mã `MH0001..MH0009`)
- Sáng: `09:00-12:00`
- Chiều: `13:15-17:15`

### 2.3 Reception
- `MH0043`, `MH0045` (lễ tân trừ Ánh Tuyết):
  - Sáng: `08:15-11:45`
  - Chiều: `13:00-17:00`
- `MH0044` (Ánh Tuyết):
  - Sáng: `09:15-13:00`
  - Chiều: `14:15-18:00`

### 2.4 PART
- Sáng: `08:30-12:00`
- Chiều: `13:15-16:30`

### 2.5 ONL
- Sáng: `08:30-12:00`
- Chiều: `13:15-16:15`

---

## 3) Quy định ca tối SALE vs Marketing (Page)

Theo yêu cầu mới:

- **SALE: KHONG setup ca tối** trong logic chấm công.
- **Marketing (team Page): CO setup ca tối**:
  - Ca tối: `18:30-22:30`
  - Ngưỡng trễ ca tối: từ `19:00` trở đi
  - Hợp lệ `5-6` mốc/ngày (1-2 sáng, 3-4 chiều, 5-6 tối)
- Team không phải Page: quá `4` mốc/ngày là problematic.

Ghi chú:
- `GIỜ-LÀM.docx` có nêu SALE tối `18:15-21:30`, nhưng theo yêu cầu nghiệp vụ hiện tại thì SALE không dùng ca tối.

---

## 4) Quy định trễ mới (lateThreshold = 30)

Áp dụng thống nhất cho OFF vân tay, ONL, OFF ngoài:

| Điều kiện trễ | Phân loại | Quy đổi lỗi |
|---|---|---|
| `0 < lateMinutes < 30` | Lỗi trễ | `1` lỗi |
| `lateMinutes >= 30` | Lỗi trễ (khong quy ve quen check) | `2` lỗi |

Nguyên tắc quan trọng:
- Khong còn logic "trễ >= 30p => quên check in/out".
- Note trễ ghi ở cột chi tiết trễ (S).
- Nhóm quên check chỉ gồm `Quên check in` / `Quên check out`.

---

## 5) Mapping cột đếm theo luồng

### OFF vân tay (raw AJ-BN)
- Cột S: chi tiết trễ
- Cột Q: tổng lỗi trễ OFF (đếm theo trọng số 1/2)
- Cột W: chi tiết quên check
- Cột U: tổng lỗi quên check OFF

### ONL
- Luồng missing: cộng vào cột V (chỉ quên check)
- Luồng late: cộng vào cột R theo trọng số 1/2
- Note trễ vẫn ghi ở S với hậu tố `(ONL)`

### OFF ngoài
- Luồng missing: cộng vào cột U (chỉ quên check OFF ngoài)
- Luồng late: cộng vào cột P theo trọng số 1/2
- Note trễ ghi ở S với hậu tố `(OFF ngoài)`

---

## 6) Check-out hợp lệ (legacy trong helper cũ)

Trong `computeSessionFromTimes_`:
- `MIN_VALID_CHECKOUT_MINUTES = 16:30`
- Một số nhánh parse cũ vẫn dùng mốc này để suy luận out hợp lệ khi dữ liệu thiếu.

---

## 7) Nhận diện role/team

- Role lấy từ cột chứa `FULL/PART/ONL/ONLINE` trong master.
- Managers nhận diện theo danh sách mã cố định, khong suy luận từ role.
- Team Page nhận diện từ cột TEAM với giá trị `page` (khong phân biệt hoa thường).

---

## 8) Vị trí code chính cần chú ý khi đổi quy định

- `SPECIAL_SCHEDULES` và `getEmployeeSchedule_`: định nghĩa giờ theo role/mã.
- `computeSessionsBySchedule_`: tách ca và xử lý case Page có ca tối.
- `handleMissingCheckInOutSimple_`: chỉ xử lý quên check.
- `handleLateSimple_`: xử lý trễ với quy đổi `>=30 => 2`.
- `applyAttendance`: tách note trễ/quên check và ghi Q/U theo quy tắc mới.
- `prepareOnlAttendanceChangesFromSheet_`, `prepareOnlAttendanceChanges_`: ONL sheet/form.
- `prepareOffAttendanceChangesFromSheet_`, `prepareOffAttendanceChanges_`: OFF ngoài sheet/form.
- `applyOnlAttendanceLateOnly`, `applyOffAttendanceLateOnly`: cột đếm trễ R/P theo trọng số.

---

Khi thay đổi nghiệp vụ tiếp theo, cần cập nhật đồng bộ:
1) giờ ca trong `SPECIAL_SCHEDULES`,
2) logic phân loại trễ vs quên check,
3) công thức đếm ở các cột tổng (Q/R/P/U/V).
