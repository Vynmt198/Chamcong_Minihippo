# FLOW VÀ LOGIC XỬ LÝ CHECK IN/OUT ONL

## TỔNG QUAN
Quy trình xử lý check-in/out ONL (Online) để phát hiện lỗi và ghi vào sheet chấm công.

---

## FLOW CHÍNH (Hàm `processOnlCheckInOut`)

### BƯỚC 1: Load dữ liệu từ Google Form (`loadOnlFormData_`)

**Input:** Form File ID: `1_mmyOMrX8cOW3bEqt6HxE5B7A0wxH5ud_SVyZEMMDQE`

**Các cột cần đọc:**
- **Cột A (index 0):** Dấu thời gian (Timestamp) - thời gian submit form
- **Cột C (index 2):** HỌ VÀ TÊN THẬT CỦA EM (Full Name)
- **Cột E (index 4):** EM CHẤM CÔNG - chứa "CA ONLINE" hoặc "CA OFFLINE"
- **Cột F (index 5):** EM CHẤM CÔNG CHO NGÀY NÀO - chứa Date object (ngày làm việc)
- **Cột G (index 6):** CA LÀM VIỆC CỦA EM - chứa "Check in ca sáng" hoặc "Check out ca chiều" (KHÔNG có ngày)

**Logic parse:**
1. Đọc từng row trong form sheet (bắt đầu từ row 2)
2. Lọc chỉ lấy các row có `checkType` chứa "ONLINE" (bỏ qua "OFFLINE")
3. Parse ngày từ cột F (Date object) → format "DD/MM"
4. Parse ca từ cột G:
   - "ca sáng" hoặc "sáng" → `shiftType = 'morning'`
   - "ca chiều" hoặc "chiều" → `shiftType = 'afternoon'`
5. Parse action từ cột G:
   - "Check in" → `action = 'in'`
   - "Check out" → `action = 'out'`
6. Parse timestamp từ cột A → Date object (thời gian check-in/out thực tế)

**Output:** Array of objects:
```javascript
{
  timestamp: Date,      // Thời gian check-in/out
  fullName: string,    // Họ tên
  checkType: string,   // "CA ONLINE"
  workShift: string,   // "Check out ca chiều"
  date: "10/4",        // "DD/MM"
  day: "10",           // Ngày
  month: "4",          // Tháng
  shiftType: "afternoon", // "morning" hoặc "afternoon"
  action: "out"        // "in" hoặc "out"
}
```

**Điều kiện để entry được thêm vào data:**
- `fullName` không rỗng
- `checkType` chứa "ONLINE"
- `date` không null (parse được từ cột F)
- `shiftType` không null (parse được từ cột G)
- `action` không null (parse được từ cột G)
- `checkTime` không null (parse được từ cột A)

---

### BƯỚC 2: Map họ tên → mã nhân viên (`buildNameToEmpMapForOnl_`)

**Input:** 
- `formData` từ bước 1
- Schedule File ID và sheet names

**Logic:**
1. Lấy tất cả tên unique từ formData
2. Đọc sheet đăng ký ca:
   - Cột C: MÃ NHÂN VIÊN (MHxxxx)
   - Cột B hoặc D: Họ tên
3. Normalize tên (uppercase, loại bỏ dấu, khoảng trắng) để so khớp
4. Tạo map: `Map<normalizedName, empCode>`

**Output:** `Map<string, string>` - Map từ tên (normalized) sang mã nhân viên

---

### BƯỚC 3: Load đăng ký ca ONL (`loadOnlScheduleRegistrations_`)

**Input:**
- Schedule File ID và sheet names
- `nameToEmpMap` từ bước 2

**Logic:**
1. Đọc sheet đăng ký ca:
   - Cột C: MÃ NHÂN VIÊN
   - Cột B: Employee Name
2. Tìm các cột ngày:
   - Row 1: Ngày (06/12/2025, 07/12/2025...)
   - Row 2: ST7, CT7, ST2, CT2... (ST = ca sáng, CT = ca chiều)
3. Với mỗi nhân viên, duyệt các cột ngày:
   - Nếu giá trị trong ô = "ONL" → thêm vào schedule
   - ST7 với "ONL" → `dayStr="7"`, `sessionName="morning"`
   - CT7 với "ONL" → `dayStr="7"`, `sessionName="afternoon"`

**Output:** `Map<empCode, Map<dayStr, Set<sessionName>>>`
- Ví dụ: `Map("MH0010", Map("7", Set("morning", "afternoon")))`

---

### BƯỚC 4: Phân tích check-in/out và phát hiện lỗi (`analyzeOnlCheckInOut_`)

**Input:**
- `formData` từ bước 1
- `nameToEmpMap` từ bước 2
- `scheduleMap` từ bước 3
- `cfg` với `morningStart: "08:15"`, `afternoonStart: "13:15"`

**Logic:**

#### 4.1. Group form data theo empCode và day
```javascript
Map<empCode, Map<dayStr, {
  morning: { in: Date|null, out: Date|null },
  afternoon: { in: Date|null, out: Date|null }
}>>
```

Với mỗi entry trong formData:
- Tìm empCode từ nameToEmpMap
- Group theo empCode → dayStr → shiftType → action
- Lưu timestamp vào `session.in` hoặc `session.out`

#### 4.2. Phát hiện lỗi

Với mỗi nhân viên có đăng ký ca ONL:
- Duyệt từng ngày có đăng ký
- Với mỗi ca đăng ký (morning/afternoon):
  
  **a) Kiểm tra quên check-in/out:**
  - Nếu `!session.in && !session.out` → Vắng hoàn toàn → `type: 'missing'`
  - Nếu `!session.in` → Quên check-in → `type: 'missing_in'`
  - Nếu `!session.out` → Quên check-out → `type: 'missing_out'`
  
  **b) Kiểm tra trễ (nếu có cả in và out):**
  - Tính `lateMinutes = checkInTime - (startTime + 30 phút)`
  - Nếu `lateMinutes > 0 && lateMinutes < 30` → `type: 'late_under_30'`
  - Nếu `lateMinutes >= 30` → `type: 'late_over_30'`

#### 4.3. Tính vắng ONL

Với mỗi nhân viên có đăng ký ca ONL:
- Nếu không có dữ liệu form cho ngày đó → vắng tất cả các ca
- Nếu có dữ liệu form nhưng `!session.in && !session.out` → vắng ca đó

**Output:**
```javascript
{
  errors: [
    {
      empCode: "MH0010",
      dayStr: "7",
      sessionName: "morning",
      type: "missing_in" | "missing_out" | "late_over_30" | ...,
      message: "Quên check in ca sáng",
      lateMinutes?: number,
      checkInTime?: Date
    }
  ],
  vangOnl: Map<empCode, count>
}
```

---

### BƯỚC 5: Ghi vào sheet chấm công

**Tìm các cột:**
- Cột W: "Chi tiết(3)" - ghi lỗi (append)
- Cột V: "ONL QUÊN CHECK IN/OUT/TRỄ >=30'" - đếm số lỗi
- Cột Z: "VẮNG ONL" - đếm số ca vắng

**Logic ghi:**

1. **Group errors theo empCode:**
   - `errorsByEmp: Map<empCode, Array<error>>`
   - `errorCountByEmp: Map<empCode, count>` (chỉ đếm `late_over_30`, `missing_in`, `missing_out`)

2. **Cập nhật cột W (Chi tiết(3)):**
   - Với mỗi nhân viên có lỗi:
     - Lấy note hiện có
     - Append các error messages: `"- Quên check in ca sáng\n- Check in trễ 35 phút ca chiều"`
     - Ghi vào sheet (append, không ghi đè)

3. **Cập nhật cột V (ONL QUÊN CHECK IN/OUT/TRỄ >=30'):**
   - Với mỗi nhân viên: ghi số lượng lỗi (chỉ đếm `late_over_30`, `missing_in`, `missing_out`)

4. **Cập nhật cột Z (VẮNG ONL):**
   - Với mỗi nhân viên: ghi số lượng ca vắng

---

## CÁC VẤN ĐỀ CÓ THỂ XẢY RA

### 1. Không load được dữ liệu từ form
**Nguyên nhân có thể:**
- Cột E không chứa "CA ONLINE" (có thể là "CA OFFLINE" hoặc giá trị khác)
- Cột F không parse được ngày
- Cột G không parse được ca/action
- Thiếu một trong các điều kiện: fullName, date, shiftType, action, checkTime

**Cách debug:**
- Kiểm tra log: `Row X: fullName="...", checkType="...", checkDate=..., workShift="..."`
- Xem giá trị thực tế của từng cột

### 2. Không map được tên → mã nhân viên
**Nguyên nhân có thể:**
- Tên trong form khác với tên trong sheet đăng ký ca
- Normalize tên không khớp

**Cách debug:**
- Kiểm tra log: `Built name to emp map: X mappings`
- So sánh tên trong form với tên trong sheet đăng ký ca

### 3. Không load được đăng ký ca ONL
**Nguyên nhân có thể:**
- Không tìm thấy cột ST/CT
- Giá trị trong ô không phải "ONL" (có thể có khoảng trắng, chữ thường...)

**Cách debug:**
- Kiểm tra log: `Found X day columns in sheet Y`
- Kiểm tra giá trị thực tế trong các ô đăng ký ca

### 4. Không phát hiện được lỗi
**Nguyên nhân có thể:**
- Nhân viên không có trong scheduleMap (không có đăng ký ca ONL)
- Dữ liệu form không match với đăng ký ca (ngày khác, ca khác)

**Cách debug:**
- Kiểm tra log: `Found X errors and Y employees with vắng ONL`
- Kiểm tra empDayData có dữ liệu không

### 5. Không ghi được vào sheet
**Nguyên nhân có thể:**
- Không tìm thấy cột W, V, Z
- Không tìm thấy mã nhân viên trong master sheet
- Lỗi khi ghi (permission, range...)

**Cách debug:**
- Kiểm tra log: `Found columns: noteCol=X, onlForgotCol=Y, vangOnlCol=Z`
- Kiểm tra log: `Updated X employees`
- Kiểm tra lỗi khi ghi

---

## ĐIỂM QUAN TRỌNG CẦN LƯU Ý

1. **Cột F chứa ngày (Date object), không phải trong cột G**
2. **Cột G chỉ có "Check in/out ca sáng/chiều", không có ngày**
3. **Chỉ xử lý các entry có "CA ONLINE", bỏ qua "CA OFFLINE"**
4. **Map tên cần normalize để so khớp tốt hơn**
5. **Đăng ký ca ONL chỉ lấy các ô có giá trị chính xác là "ONL"**
6. **Ghi vào cột W là append, không ghi đè**


