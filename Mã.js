/**
 * IMPORT RAWLOG (nhiều cơ sở) -> SHEET TỔNG (CHẤM CÔNG 2025)
 * Rawlog dạng block: mỗi nhân viên 1 block, header ngày 1..31, mỗi ô chứa nhiều giờ xuống dòng
 */
function importAllBranchesRawLogToMaster() {
  // ====== CONFIG ======
  const RAW_FILE_ID = "1ed1IK4X1bQxhBoz4tjUKEypIv6cipNKsUCcXPKjqy8o";
  // Test 1 cơ sở trước:
  // const RAW_SHEETS = ["L4_HH"];
  // Khi OK thì bật đủ:
  const RAW_SHEETS = ["L4_HH", "L1_HH", "L5_HH", "HDK", "TP"];

  const MASTER_FILE_ID = "1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4";  
  const MASTER_SHEET_NAME = "Chấm công th12/2025";

  const MASTER_EMP_COL = 2;     // cột mã nhân viên (B)
  const MASTER_HEADER_ROW = 1;  // hàng chứa số ngày 1..31
  // Khối cột ghi rawlog vân tay: bắt buộc từ AJ (36) đến BN (66)
  const MASTER_DAY_FIRST_COL = 36;  // AJ
  const MASTER_DAY_LAST_COL = 66;   // BN

  // ====== 1) OPEN RAW FILE ======
  Logger.log("1) Open raw file...");
  const rawSS = SpreadsheetApp.openById(RAW_FILE_ID);

  // timesByEmpDay: Map<empCode, Map<dayStr, Set(times)>>
  const timesByEmpDay = new Map();

  // ====== 2) PARSE ALL BRANCH SHEETS ======
  Logger.log("2) Parse branch sheets...");
  RAW_SHEETS.forEach((name, idx) => {
    const sh = rawSS.getSheetByName(name);
    if (!sh) throw new Error("Không tìm thấy sheet rawlog: " + name);

    const lr = sh.getLastRow();
    const lc = sh.getLastColumn();
    const values = sh.getRange(1, 1, lr, lc).getValues();

    Logger.log(`   - Parsing ${name} (${idx + 1}/${RAW_SHEETS.length}) rows=${lr}, cols=${lc}`);
    parseRawLogValuesIntoMap_(values, timesByEmpDay);
  });

  Logger.log("timesByEmpDay size=" + timesByEmpDay.size);

  // ====== 3) OPEN MASTER FILE ======
  Logger.log("3) Open master file...");
  const masterSS = SpreadsheetApp.openById(MASTER_FILE_ID);
  const masterSh = masterSS.getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) throw new Error("Không tìm thấy sheet tổng: " + MASTER_SHEET_NAME);

  // ====== 4) BUILD rowByEmp ======
  const lastEmpRow = findLastEmployeeRow_(masterSh, MASTER_EMP_COL);
  Logger.log("lastEmpRow=" + lastEmpRow);

  const empRegex = /^MH\d{4}$/i;
  const empColVals = masterSh.getRange(1, MASTER_EMP_COL, lastEmpRow, 1).getValues().flat();

  const rowByEmp = new Map(); // emp -> row (1-based)
  empColVals.forEach((v, idx) => {
    const emp = String(v || "").trim().toUpperCase();
    if (empRegex.test(emp)) rowByEmp.set(emp, idx + 1);
  });
  Logger.log("rowByEmp size=" + rowByEmp.size);

  // ====== 5) BUILD colByDay: cố định theo vị trí AJ=ngày 1 .. BN=ngày 31 ======
  const minDayCol = MASTER_DAY_FIRST_COL;
  const maxDayCol = MASTER_DAY_LAST_COL;
  const colByDay = new Map(); // dayStr -> col (1-based)
  for (let d = 1; d <= 31; d++) {
    colByDay.set(String(d), MASTER_DAY_FIRST_COL + (d - 1));
    if (d <= 9) colByDay.set("0" + d, MASTER_DAY_FIRST_COL + (d - 1)); // raw có thể gửi "01".."09"
  }

  Logger.log("colByDay size=" + colByDay.size + ", day block cols " + minDayCol + ".." + maxDayCol + " (AJ..BN)");

  // ====== 6) READ dayBlock ONCE ======
  const dayColsCount = maxDayCol - minDayCol + 1;
  Logger.log(`4) Read dayBlock once: rows=${lastEmpRow}, cols=${dayColsCount} (col ${minDayCol}..${maxDayCol})`);

  const dayBlockRange = masterSh.getRange(1, minDayCol, lastEmpRow, dayColsCount);
  const dayBlock = dayBlockRange.getValues(); // 2D [row][col]

  // ====== 7) UPDATE IN MEMORY ======
  Logger.log("5) Update in memory...");
  let updatedCells = 0;
  const notFound = [];

  for (const [emp, dayMap] of timesByEmpDay.entries()) {
    const r1 = rowByEmp.get(emp);
    if (!r1) {
      notFound.push(emp);
      continue;
    }

    const r0 = r1 - 1; // 0-based index for arrays

    for (const [dayStr, timeSet] of dayMap.entries()) {
      const col1 = colByDay.get(dayStr);
      if (!col1) continue;

      const c0 = col1 - minDayCol; // 0-based within dayBlock
      if (c0 < 0 || c0 >= dayColsCount) continue;

      const existing = String(dayBlock[r0][c0] || "").trim();
      const merged = mergeTimes_(extractTimes_(existing), Array.from(timeSet));
      const newText = merged.join("\n");

      if (newText !== existing) {
        dayBlock[r0][c0] = newText;
        updatedCells++;
      }
    }
  }

  // ====== 8) WRITE BACK IN BATCHES ======
  Logger.log(`6) Write back in batches... updatedCells=${updatedCells}, notFound=${notFound.length}`);
  let errorCount = 0; // Khai báo ở ngoài để dùng trong alert
  if (updatedCells > 0) {
    // Tối ưu: Chia nhỏ range thành các batch để tránh timeout
    // Google Apps Script có giới hạn thời gian thực thi, nên chia nhỏ range
    const BATCH_SIZE = 50; // Số hàng mỗi batch (có thể điều chỉnh)
    const totalRows = lastEmpRow;
    let batchCount = 0;
    let successCount = 0;

    try {
      for (let startRow = 1; startRow <= totalRows; startRow += BATCH_SIZE) {
        const endRow = Math.min(startRow + BATCH_SIZE - 1, totalRows);
        const batchRows = endRow - startRow + 1;
        
        // Lấy phần dayBlock tương ứng với batch này
        const batchData = [];
        for (let r = startRow - 1; r < endRow; r++) {
          batchData.push(dayBlock[r] || []);
        }

        try {
          // Ghi batch này
          const batchRange = masterSh.getRange(startRow, minDayCol, batchRows, dayColsCount);
          batchRange.setValues(batchData);
          
          // Force flush sau mỗi batch để đảm bảo dữ liệu được ghi
          SpreadsheetApp.flush();
          
          batchCount++;
          successCount += batchRows;
          Logger.log(`  ✓ Batch ${batchCount}: rows ${startRow}-${endRow} (${batchRows} rows)`);
          
          // Nghỉ ngắn giữa các batch để tránh rate limit
          if (batchCount % 5 === 0) {
            Utilities.sleep(100); // 100ms nghỉ sau mỗi 5 batch
          }
        } catch (batchError) {
          errorCount++;
          Logger.log(`  ✗ ERROR in batch ${batchCount} (rows ${startRow}-${endRow}): ${batchError.message}`);
          // Tiếp tục với batch tiếp theo thay vì dừng hoàn toàn
        }
      }

      Logger.log(`6a) Write completed: ${batchCount} batches, ${successCount} rows written, ${errorCount} errors`);
      
      if (errorCount > 0) {
        Logger.log(`WARNING: ${errorCount} batch(es) failed. Some data may not be updated.`);
      }
      
      // QUAN TRỌNG: Flush cuối cùng để đảm bảo tất cả dữ liệu được commit vào sheet
      // Trước khi hiển thị alert, phải đảm bảo dữ liệu đã được ghi xong
      SpreadsheetApp.flush();
      Logger.log("6b) Final flush completed - all data committed to sheet");
      
      // Delay ngắn để đảm bảo Google Sheets có thời gian refresh UI
      // Dữ liệu đã được ghi vào sheet, delay này chỉ để UI refresh
      Utilities.sleep(200); // 200ms delay
      
    } catch (e) {
      Logger.log(`CRITICAL ERROR during batch write: ${e.message}`);
      Logger.log(`Stack: ${e.stack}`);
      throw new Error(`Failed to write data: ${e.message}`);
    }
  } else {
    Logger.log("No changes -> skip setValues()");
  }

  if (notFound.length) Logger.log("Không tìm thấy mã trong sheet tổng: " + notFound.join(", "));

  // Hiển thị thông báo bằng toast (không chặn execution, dữ liệu đã được ghi)
  // Toast sẽ tự động biến mất sau vài giây, không cần user bấm OK
  try {
    const message = `Đã cập nhật ${updatedCells} ô` +
      (notFound.length ? ` (${notFound.length} mã không tìm thấy)` : "") +
      (errorCount > 0 ? ` (${errorCount} batch lỗi)` : "");
    
    // Sử dụng toast thay vì alert - toast không chặn execution
    masterSh.getRange(1, 1).setValue(masterSh.getRange(1, 1).getValue()); // Trigger refresh
    SpreadsheetApp.getActiveSpreadsheet().toast(message, "Hoàn thành", 5); // 5 giây
    
    Logger.log(`Toast notification: ${message}`);
  } catch (e) {
    // Fallback: Nếu toast không hoạt động, chỉ log
    Logger.log(`Notification skipped. Finished: Updated ${updatedCells} ô.` +
      (notFound.length ? ` Không tìm thấy ${notFound.length} mã.` : "") +
      (errorCount > 0 ? ` ${errorCount} batch errors.` : ""));
  }
}
/**
 * Parse sheet rawlog dạng report block -> timesByEmpDay
 * Block bắt đầu ở dòng có "Mã số" (cột A)
 * Trong block có mã MHxxxx
 * Dòng kế là header ngày 1..31
 * Các dòng sau chứa times HH:mm ở từng cột ngày
 */
function parseRawLogValuesIntoMap_(rawValues, timesByEmpDay) {
  const empRegex = /^MH\d{4}$/i;
  const timeRegex = /\b([01]?\d|2[0-3]):[0-5]\d\b/g;

  const isBlockStart = (row) => normalize_(row[0]) === "ma so";

  let r = 0;
  while (r < rawValues.length) {
    if (!isBlockStart(rawValues[r])) { r++; continue; }

    const headerDaysRow = r + 1;
    const dataStartRow = r + 3;

    // Tìm mã nhân viên MHxxxx trong 0..3 dòng đầu block
    let empCode = "";
    for (let rr = r; rr <= Math.min(r + 3, rawValues.length - 1); rr++) {
      for (let cc = 0; cc < rawValues[rr].length; cc++) {
        const v = String(rawValues[rr][cc] || "").trim();
        if (empRegex.test(v)) { empCode = v.toUpperCase(); break; }
      }
      if (empCode) break;
    }
    if (!empCode) { r++; continue; }

    // Map cột -> ngày
    const colToDay = new Map();
    if (headerDaysRow < rawValues.length) {
      const dayRow = rawValues[headerDaysRow];
      for (let c = 0; c < dayRow.length; c++) {
        const day = parseDayFromValue_(dayRow[c]);
        if (day) colToDay.set(c, day);
      }
    }
    if (colToDay.size === 0) { r++; continue; }

    // duyệt các dòng trong block cho tới khi gặp block tiếp theo
    let rr = dataStartRow;
    while (rr < rawValues.length && !isBlockStart(rawValues[rr])) {
      const row = rawValues[rr];

      for (const [c, dayStr] of colToDay.entries()) {
        const cell = row[c];
        if (!cell) continue;

        const matches = extractTimesFromCell_(cell);
        if (!matches.length) continue;

        if (!timesByEmpDay.has(empCode)) timesByEmpDay.set(empCode, new Map());
        const dayMap = timesByEmpDay.get(empCode);

        if (!dayMap.has(dayStr)) dayMap.set(dayStr, new Set());
        const set = dayMap.get(dayStr);

        matches.forEach(t => set.add(t));
      }

      rr++;
    }

    r = rr; // nhảy qua block tiếp theo
  }
}

/**
 * Parse một giá trị cell thành số ngày (1-31) nếu có thể
 * Hỗ trợ Date object, number, hoặc string dạng "1", "02", "31"
 * @param {*} value - Giá trị cần parse
 * @return {string|null} - Số ngày dạng string "1".."31" hoặc null
 */
function parseDayFromValue_(value) {
  if (value instanceof Date && !isNaN(value.getTime())) {
    return String(value.getDate());
  }
  if (typeof value === "number" && value >= 1 && value <= 31) {
    return String(Math.trunc(value));
  }
  const s = String(value || "").trim();
  if (/^\d{1,2}$/.test(s)) {
    const dn = Number(s);
    if (dn >= 1 && dn <= 31) return String(dn);
  }
  // Fallback: header có thể là "5 - T5", "Ngày 5", "05" → trích số ngày đầu tiên 1-31
  const m = s.match(/\b(\d{1,2})\b/);
  if (m) {
    const dn = Number(m[1]);
    if (dn >= 1 && dn <= 31) return String(dn);
  }
  return null;
}

function extractTimes_(text) {
  if (!text) return [];
  const m = String(text).match(/\b([01]?\d|2[0-3]):[0-5]\d\b/g);
  return m ? m : [];
}

function mergeTimes_(arr1, arr2) {
  const pad = (t) => {
    const m = String(t).trim().match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
    if (!m) return String(t).trim();
    return (m[1].length === 1 ? '0' + m[1] : m[1]) + ':' + m[2];
  };
  const toMin = (t) => {
    const m = t.match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
    if (!m) return 24 * 60;
    return Number(m[1]) * 60 + Number(m[2]);
  };
  const seen = new Set();
  const out = [];
  [...(arr1 || []), ...(arr2 || [])].forEach(t => {
    let tt = String(t || "").trim();
    if (!tt) return;
    tt = pad(tt);
    if (!seen.has(tt)) { seen.add(tt); out.push(tt); }
  });
  out.sort((a, b) => toMin(a) - toMin(b));
  return out;
}

function normalize_(v) {
  const s = String(v || "").trim().toLowerCase();
  return s
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ");
}

/**
 * Tìm dòng cuối cùng có mã MHxxxx trong cột mã nhân viên để tránh getLastRow() bị kéo bởi format
 */
function findLastEmployeeRow_(sheet, empCol) {
  const lr = sheet.getLastRow();
  const colVals = sheet.getRange(1, empCol, lr, 1).getValues().flat();
  const empRegex = /^MH\d{4}$/i;

  for (let i = colVals.length - 1; i >= 0; i--) {
    const v = String(colVals[i] || "").trim();
    if (empRegex.test(v)) return i + 1;
  }
  return 1;
}
function extractTimesFromCell_(cell) {
  // Trả về array ["08:19","12:04",...]
  const tz = Session.getScriptTimeZone();

  // Nếu là Date object
  if (Object.prototype.toString.call(cell) === "[object Date]" && !isNaN(cell.getTime())) {
    return [Utilities.formatDate(cell, tz, "HH:mm")];
  }

  // Nếu là number (Google Sheets time serial)
  if (typeof cell === "number" && isFinite(cell)) {
    // 0.5 ~ 12:00, 0.34 ~ 08:10...
    const ms = Math.round(cell * 24 * 60 * 60 * 1000);
    const d = new Date(ms);
    return [Utilities.formatDate(d, "UTC", "HH:mm")]; // UTC để không lệch múi giờ
  }

  // Nếu là string (có thể nhiều dòng)
  const text = String(cell || "").trim();
  if (!text) return [];
  const m = text.match(/\b([01]?\d|2[0-3]):[0-5]\d\b/g);
  return m ? m : [];
}

// ----------------------- Attendance summary (preview & apply) -----------------------
function parseMonthFromSheetName_(name) {
  const m = String(name || "").toLowerCase().match(/th\s*(\d{1,2})/);
  return m ? Number(m[1]) : null;
}

function timeStrToMinutes_(hhmm) {
  const m = String(hhmm || "").match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
  if (!m) return null;
  return Number(m[1]) * 60 + Number(m[2]);
}

function computeSessionFromTimes_(timesArr, sessionStartMin) {
  // timesArr already sorted as HH:mm
  // Legacy function - updated to match new logic: checkout from 16:30 onwards is valid
  const MIN_VALID_CHECKOUT_MINUTES = 16 * 60 + 30; // 16:30 = 990 minutes
  if (!timesArr || timesArr.length === 0) return { in: null, out: null, missingIn: true, missingOut: true, lateMinutes: 0 };
  const inStr = timesArr[0];
  // If only one time entry, treat as check-in only (missing checkout) unless it's >= 16:30
  if (timesArr.length === 1) {
    const inMin = timeStrToMinutes_(inStr);
    const lateMinutes = inMin !== null ? Math.max(0, inMin - sessionStartMin) : 0;
    const isValidCheckout = inMin !== null && inMin >= MIN_VALID_CHECKOUT_MINUTES;
    return {
      in: isValidCheckout ? null : inStr,
      out: isValidCheckout ? inStr : null,
      missingIn: isValidCheckout ? true : (inStr == null),
      missingOut: !isValidCheckout,
      lateMinutes: isValidCheckout ? 0 : lateMinutes
    };
  }
  const outStr = timesArr[timesArr.length - 1];
  const inMin = timeStrToMinutes_(inStr);
  const outMin = timeStrToMinutes_(outStr);
  const lateMinutes = inMin !== null ? Math.max(0, inMin - sessionStartMin) : 0;
  // Check if checkout is valid: must be >= 16:30
  const isValidCheckout = outMin !== null && outMin >= MIN_VALID_CHECKOUT_MINUTES;
  return {
    in: inStr,
    out: isValidCheckout ? outStr : null,
    missingIn: inStr == null,
    missingOut: !isValidCheckout,
    lateMinutes
  };
}

// --- Helper: build times map from raw sheets ---
function buildTimesFromRawSheets_(rawFileId, rawSheetNames) {
  const rawSS = SpreadsheetApp.openById(rawFileId);
  const timesByEmpDay = new Map();
  rawSheetNames.forEach(name => {
    const sh = rawSS.getSheetByName(name);
    if (!sh) return;
    parseRawLogValuesIntoMap_(sh.getDataRange().getValues(), timesByEmpDay);
  });
  return timesByEmpDay;
}

// --- Helper: index master sheet and return useful info ---
// dayColMin, dayColMax (optional): chỉ lấy cột ngày trong khoảng [dayColMin, dayColMax] (1-based).
// Ví dụ: dữ liệu vân tay ở cột AJ–BN thì truyền 36, 66 để tránh nhầm với khối cột khác có header 1..31.
function buildMasterInfo_(masterSh, masterEmpCol, masterHeaderRow, dayColMin, dayColMax) {
  const lastEmpRow = findLastEmployeeRow_(masterSh, masterEmpCol);
  const empColVals = masterSh.getRange(1, masterEmpCol, lastEmpRow, 1).getValues().flat();
  const empToRow = new Map();
  const empRegex = /^MH\d{4}$/i;
  empColVals.forEach((v, idx) => { const emp = String(v || "").trim().toUpperCase(); if (empRegex.test(emp)) empToRow.set(emp, idx + 1); });

  const lastCol = masterSh.getLastColumn();
  const header = masterSh.getRange(masterHeaderRow, 1, 1, lastCol).getValues()[0];

  const colByDay = new Map();
  let minDayCol = null, maxDayCol = null;
  for (let c = 0; c < header.length; c++) {
    const col1 = c + 1;
    if (dayColMin != null && dayColMax != null && (col1 < dayColMin || col1 > dayColMax)) continue;
    const day = parseDayFromValue_(header[c]);
    if (day) {
      colByDay.set(day, col1);
      if (minDayCol === null || col1 < minDayCol) minDayCol = col1;
      if (maxDayCol === null || col1 > maxDayCol) maxDayCol = col1;
    }
  }

  if (minDayCol === null) throw new Error("No day columns found in master");
  const dayColsCount = maxDayCol - minDayCol + 1;
  const dayBlock = masterSh.getRange(1, minDayCol, lastEmpRow, dayColsCount).getValues();

  // Try to detect a column that contains role (FULL/PART/ONL/QL) so we can map employees to roles
  let roleCol = null;
  for (let c = 0; c < header.length; c++) {
    const h = String(header[c] || '').toLowerCase();
    if (h.includes('full') && h.includes('part')) { roleCol = c + 1; break; }
  }
  if (roleCol === null) {
    for (let c = 0; c < header.length; c++) {
      const h = String(header[c] || '').toLowerCase();
      if (h.includes('full') || h.includes('part') || h.includes('onl') || h.includes('on-line') || h.includes('online')) { roleCol = c + 1; break; }
    }
  }

  const empToRole = new Map();
  if (roleCol !== null) {
    const roleVals = masterSh.getRange(1, roleCol, lastEmpRow, 1).getValues().flat();
    for (const [emp, row] of empToRow.entries()) {
      const v = String(roleVals[row - 1] || '').trim();
      if (v) empToRole.set(emp, v);
    }
  }

  return { lastEmpRow, empToRow, colByDay, minDayCol, maxDayCol, dayColsCount, dayBlock, header, empToRole };
}

// --- Helper: compute morning/afternoon sessions from times array ---
function computeAttendanceSessionsForTimes_(times, cfg) {
  const timesSorted = (times || []).slice().map(t => {
    const m = String(t).match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
    return m ? ((m[1].length === 1 ? '0' + m[1] : m[1]) + ':' + m[2]) : String(t);
  }).sort((a, b) => (timeStrToMinutes_(a) || 0) - (timeStrToMinutes_(b) || 0));

  const cutoffMin = timeStrToMinutes_(cfg.cutoff);
  const morningTimes = timesSorted.filter(t => (timeStrToMinutes_(t) || 0) < cutoffMin);
  const afternoonTimes = timesSorted.filter(t => (timeStrToMinutes_(t) || 0) >= cutoffMin);

  return {
    morning: computeSessionFromTimes_(morningTimes, timeStrToMinutes_(cfg.morningStart)),
    afternoon: computeSessionFromTimes_(afternoonTimes, timeStrToMinutes_(cfg.afternoonStart))
  };
}

// --- Helper: generate notes for a day based on sessions ---
function generateNotesForDay_(emp, dayStr, morning, afternoon, cfg, month) {
  const notes = [];
  if (morning.in == null) notes.push(`- Quên check in ca sáng ${dayStr}/${month}`);
  if (morning.out == null && morning.in != null) notes.push(`- Quên check out ca sáng ${dayStr}/${month}`);
  // Note: "Trễ" logic removed
  if (afternoon.in == null) notes.push(`- Quên check in ca chiều ${dayStr}/${month}`);
  if (afternoon.out == null && afternoon.in != null) notes.push(`- Quên check out ca chiều ${dayStr}/${month}`);
  // Note: "Trễ" logic removed
  return notes;
}

// --- Special schedules & helpers for exceptions ---
const SPECIAL_SCHEDULES = {
  // Nhân viên bình thường: Ca sáng 8h30-12h, Ca chiều 13h15-16h45
  default: { morningStart: "08:30", morningEnd: "12:00", afternoonStart: "13:15", afternoonEnd: "16:45", cutoff: "12:00", lateThreshold: 30 },
  // Quản lý (MH0001-MH0009): Ca sáng 9h00-12h, Ca chiều 13h15-17h15
  managers: {
    ids: ["MH0001", "MH0002", "MH0003", "MH0004", "MH0005", "MH0006", "MH0007", "MH0008", "MH0009"],
    template: { morningStart: "09:00", morningEnd: "12:00", afternoonStart: "13:15", afternoonEnd: "17:15" }
  },
  reception: {
    ids: ["MH0043", "MH0044", "MH0045"],
    mapping: { "MH0043": "caDoi12", "MH0044": "caDoi23", "MH0045": "caLe" },
    templates: {
      caDoi12: { sessions: [{ name: 'ca_sang', start: '08:15', end: '12:15' }, { name: 'ca_chieu', start: '13:15', end: '16:30' }] },
      caDoi23: { sessions: [{ name: 'ca_chieu_1', start: '12:15', end: '16:15' }, { name: 'ca_chieu_2', start: '16:15', end: '20:00' }] },
      caLe: { sessions: [{ name: 'ca_s1', start: '08:15', end: '12:15' }, { name: 'ca_s2', start: '12:15', end: '16:15' }, { name: 'ca_s3', start: '16:15', end: '20:15' }] }
    }
  },
  // Add role-driven templates that can be customized later
  parttime: {
    // PART: vẫn tính lịch như nhân viên thường để phát hiện trễ vân tay
    // (thực tế vận hành: ca chiều bắt đầu 13:15; nếu cần khác theo nhóm PART thì sẽ cấu hình riêng theo mã NV)
    template: { morningStart: "08:30", morningEnd: "12:00", afternoonStart: "13:15", afternoonEnd: "16:30" }
  },
  online: {
    template: { morningStart: "08:30", morningEnd: "12:00", afternoonStart: "13:15", afternoonEnd: "16:15" }
  }
};

function getEmployeeSchedule_(emp, baseCfg, role) {
  // baseCfg contains default fields
  const empId = String(emp || "").toUpperCase();
  const roleStr = String(role || "").trim().toUpperCase();
  // YÊU CẦU: ca chiều bắt đầu cố định 13:15 cho mọi role (trừ các schedule đặc thù kiểu lễ tân dùng sessions riêng)
  const FIXED_AFTERNOON_START = "13:15";

  // Role-based override (column F / FULL/PART)
  if (roleStr) {
    // IMPORTANT: không suy luận quản lý từ roleStr vì dễ bị sai dữ liệu/nhầm cột role → gây sót lỗi trễ.
    // Quản lý chỉ được override bằng danh sách mã (SPECIAL_SCHEDULES.managers.ids) ở phía dưới.

    if (roleStr.includes('PART')) {
      const t = SPECIAL_SCHEDULES.parttime.template || SPECIAL_SCHEDULES.default;
      return {
        useHalfDaySplit: true,
        morning: { start: t.morningStart || SPECIAL_SCHEDULES.default.morningStart, end: t.morningEnd || SPECIAL_SCHEDULES.default.morningEnd },
        afternoon: { start: FIXED_AFTERNOON_START, end: t.afternoonEnd || SPECIAL_SCHEDULES.default.afternoonEnd },
        cutoff: baseCfg.cutoff || SPECIAL_SCHEDULES.default.cutoff,
        lateThreshold: baseCfg.lateThreshold || SPECIAL_SCHEDULES.default.lateThreshold
      };
    }

    if (roleStr.includes('ONL') || roleStr.includes('ONLINE')) {
      const t = SPECIAL_SCHEDULES.online.template || SPECIAL_SCHEDULES.default;
      return {
        useHalfDaySplit: true,
        morning: { start: t.morningStart || SPECIAL_SCHEDULES.default.morningStart, end: t.morningEnd || SPECIAL_SCHEDULES.default.morningEnd },
        afternoon: { start: FIXED_AFTERNOON_START, end: t.afternoonEnd || SPECIAL_SCHEDULES.default.afternoonEnd },
        cutoff: baseCfg.cutoff || SPECIAL_SCHEDULES.default.cutoff,
        lateThreshold: baseCfg.lateThreshold || SPECIAL_SCHEDULES.default.lateThreshold
      };
    }
  }

  // Managers override by ID (if role was not present or didn't indicate manager)
  if (SPECIAL_SCHEDULES.managers.ids.includes(empId)) {
    const t = SPECIAL_SCHEDULES.managers.template;
    return {
      useHalfDaySplit: true,
      morning: { start: t.morningStart, end: t.morningEnd },
      afternoon: { start: FIXED_AFTERNOON_START, end: t.afternoonEnd },
      cutoff: baseCfg.cutoff || SPECIAL_SCHEDULES.default.cutoff,
      lateThreshold: baseCfg.lateThreshold || SPECIAL_SCHEDULES.default.lateThreshold
    };
  }

  // Reception mapping (use explicit sessions)
  if (SPECIAL_SCHEDULES.reception.ids.includes(empId)) {
    const mapName = SPECIAL_SCHEDULES.reception.mapping[empId];
    const tpl = SPECIAL_SCHEDULES.reception.templates[mapName];
    if (tpl && tpl.sessions) {
      return { useHalfDaySplit: false, sessions: tpl.sessions, cutoff: baseCfg.cutoff || SPECIAL_SCHEDULES.default.cutoff, lateThreshold: baseCfg.lateThreshold || SPECIAL_SCHEDULES.default.lateThreshold };
    }
  }

  // default (use half-day split)
  return {
    useHalfDaySplit: true,
    morning: { start: baseCfg.morningStart || SPECIAL_SCHEDULES.default.morningStart, end: baseCfg.morningEnd || SPECIAL_SCHEDULES.default.morningEnd },
    afternoon: { start: FIXED_AFTERNOON_START, end: baseCfg.afternoonEnd || SPECIAL_SCHEDULES.default.afternoonEnd },
    cutoff: baseCfg.cutoff || SPECIAL_SCHEDULES.default.cutoff,
    lateThreshold: baseCfg.lateThreshold || SPECIAL_SCHEDULES.default.lateThreshold
  };
}

function computeSessionsBySchedule_(times, schedule, registeredSessions) {
  /**
   * LOGIC MỚI - Xác định check in/out cho ca sáng và ca chiều dựa trên số mốc thời gian
   * 
   * QUY TẮC MỚI:
   * - Ca sáng: 08:15-12:00
   * - Ca chiều: 13:15-17:00
   * - Check-in ca sáng: 07:30-08:30 (hợp lệ)
   * - Check-out ca sáng: 11:30-12:30 (hợp lệ)
   * - Check-in ca chiều: 12:30-13:30 (hợp lệ)
   * - Check-out ca chiều: 16:30-trở đi (hợp lệ)
   * 
   * XỬ LÝ SỐ MỐC:
   * - 2 mốc: Dựa vào đăng ký ca để xác định (nếu đăng ký ca sáng thì mốc 1=check-in sáng, mốc 2=check-out sáng;
   *   nếu chỉ đăng ký ca chiều thì mốc 1=check-in chiều, mốc 2=check-out chiều)
   * - 4 mốc: mốc 1=check-in sáng, mốc 2=check-out sáng, mốc 3=check-in chiều, mốc 4=check-out chiều
   * - 3 mốc: Phân tích logic để xác định quên check-out sáng hoặc quên check-in chiều
   * - >4 mốc: Đánh dấu để check thủ công (trả về problematic)
   */

  const out = {};
  const timesSorted = (times || []).slice().map(t => {
    const m = String(t).match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
    return m ? ((m[1].length === 1 ? '0' + m[1] : m[1]) + ':' + m[2]) : String(t);
  }).sort((a, b) => (timeStrToMinutes_(a) || 0) - (timeStrToMinutes_(b) || 0));

  if (timesSorted.length === 0) {
    // Không có giờ check nào
    if (schedule.useHalfDaySplit) {
      out['morning'] = { times: [], in: null, out: null, missingIn: true, missingOut: true, lateMinutes: 0, earlyMinutes: 0 };
      out['afternoon'] = { times: [], in: null, out: null, missingIn: true, missingOut: true, lateMinutes: 0, earlyMinutes: 0 };
    }
    return out;
  }

  // Đánh dấu problematic nếu >4 mốc
  if (timesSorted.length > 4) {
    out['_problematic'] = true;
    out['_timesCount'] = timesSorted.length;
    // Vẫn trả về kết quả mặc định để không bị lỗi
    if (schedule.useHalfDaySplit) {
      out['morning'] = { times: [], in: null, out: null, missingIn: true, missingOut: true, lateMinutes: 0, earlyMinutes: 0 };
      out['afternoon'] = { times: [], in: null, out: null, missingIn: true, missingOut: true, lateMinutes: 0, earlyMinutes: 0 };
    }
    return out;
  }

  // Nếu schedule không dùng half-day split (reception), xử lý riêng
  if (!schedule.useHalfDaySplit) {
    // Xử lý cho reception với explicit sessions (giữ nguyên logic cũ)
    const sessDefs = schedule.sessions.map(s => ({ name: s.name, startMin: timeStrToMinutes_(s.start), endMin: timeStrToMinutes_(s.end), raw: s }));
    const buckets = {};
    sessDefs.forEach(sd => { buckets[sd.name] = []; });

    timesSorted.forEach(t => {
      const tmin = timeStrToMinutes_(t) || 0;
      let assigned = false;
      for (const sd of sessDefs) {
        if (tmin >= sd.startMin && tmin <= sd.endMin) { buckets[sd.name].push(t); assigned = true; break; }
      }
      if (!assigned) {
        let best = null, bestDist = Infinity;
        for (const sd of sessDefs) {
          const dist = Math.abs(tmin - sd.startMin);
          if (dist < bestDist) { bestDist = dist; best = sd; }
        }
        if (best && bestDist <= 60) buckets[best.name].push(t);
      }
    });

    for (const sd of sessDefs) {
      const arr = buckets[sd.name];
      if (!arr || arr.length === 0) {
        out[sd.name] = { times: [], in: null, out: null, missingIn: true, missingOut: true, lateMinutes: 0, earlyMinutes: 0 };
        continue;
      }

      const sessionTimes = arr.slice().sort((a, b) => (timeStrToMinutes_(a) || 0) - (timeStrToMinutes_(b) || 0));
      let sessionIn = null, sessionOut = null;

      if (sessionTimes.length === 1) {
        const singleTimeMin = timeStrToMinutes_(sessionTimes[0]);
        if (singleTimeMin !== null && singleTimeMin >= sd.startMin - 30 && singleTimeMin <= sd.startMin + 60) {
          sessionIn = sessionTimes[0];
        } else if (singleTimeMin !== null && singleTimeMin >= sd.endMin - 60) {
          // Có thể là check-out nhưng không có check-in
        } else {
          sessionIn = sessionTimes[0];
        }
      } else if (sessionTimes.length >= 2) {
        sessionIn = sessionTimes[0];
        const firstTimeMin = timeStrToMinutes_(sessionIn);
        if (firstTimeMin !== null && firstTimeMin < sd.startMin - 30 && sessionTimes.length > 1) {
          sessionIn = sessionTimes[1];
        }
        if (sessionIn) {
          const inMin = timeStrToMinutes_(sessionIn);
          for (let i = sessionTimes.length - 1; i >= 0; i--) {
            const tMin = timeStrToMinutes_(sessionTimes[i]);
            if (tMin !== null && inMin !== null && tMin > inMin + 15) {
              sessionOut = sessionTimes[i];
              break;
            }
          }
          if (!sessionOut && sessionTimes.length >= 2) {
            const lastTimeMin = timeStrToMinutes_(sessionTimes[sessionTimes.length - 1]);
            if (lastTimeMin !== null && inMin !== null && lastTimeMin > inMin) {
              sessionOut = sessionTimes[sessionTimes.length - 1];
            }
          }
        }
      }

      // Tính lateMinutes: số phút trễ so với giờ bắt đầu ca
      let lateMinutes = 0;
      if (sessionIn) {
        const sessionInMin = timeStrToMinutes_(sessionIn);
        if (sessionInMin !== null && sd.startMin !== null) {
          if (sessionInMin > sd.startMin) {
            lateMinutes = sessionInMin - sd.startMin;
          }
        }
      }
      out[sd.name] = {
        times: sessionTimes,
        in: sessionIn,
        out: sessionOut,
        missingIn: sessionIn === null,
        missingOut: sessionIn !== null && sessionOut === null,
        lateMinutes,
        earlyMinutes: 0
      };
    }

    return out;
  }

  // LOGIC CẢI TIẾN: Xử lý half-day split theo số mốc thời gian
  // Định nghĩa các khoảng thời gian hợp lệ dựa trên schedule thực tế của từng role
  // Ca sáng: 8h30-12h (nhân viên) hoặc 9h00-12h (quản lý)
  // Ca chiều: 13h15-16h45 (nhân viên) hoặc 13h15-17h15 (quản lý)
  
  // Kiểm tra an toàn: fallback về default nếu schedule không có morning/afternoon
  const morningStart = schedule.morning?.start || SPECIAL_SCHEDULES.default.morningStart;
  const morningEnd = schedule.morning?.end || SPECIAL_SCHEDULES.default.morningEnd;
  const afternoonStart = schedule.afternoon?.start || SPECIAL_SCHEDULES.default.afternoonStart;
  const afternoonEnd = schedule.afternoon?.end || SPECIAL_SCHEDULES.default.afternoonEnd;
  
  const morningStartMin = timeStrToMinutes_(morningStart);
  const morningEndMin = timeStrToMinutes_(morningEnd);
  const afternoonStartMin = timeStrToMinutes_(afternoonStart);
  const afternoonEndMin = timeStrToMinutes_(afternoonEnd);
  
  // Khoảng thời gian hợp lệ cho check-in/out (linh hoạt để xử lý các trường hợp edge case)
  // Check-in sáng: từ 1.5 giờ trước giờ bắt đầu ca đến 1.5 giờ sau
  const MORNING_CHECKIN_EARLY = Math.max(0, (morningStartMin || 510) - 90); // 90 phút trước
  const MORNING_CHECKIN_LATE = (morningStartMin || 510) + 90; // 90 phút sau
  // Check-out sáng: từ 2 giờ sau check-in đến 1.5 giờ sau giờ kết thúc ca
  const MORNING_CHECKOUT_EARLY = (morningStartMin || 510) + 120; // Ít nhất 2 giờ sau check-in
  const MORNING_CHECKOUT_LATE = (morningEndMin || 720) + 90; // 90 phút sau giờ kết thúc ca

  // Check-in chiều: từ sau giờ kết thúc ca sáng (hoặc từ 12:00) đến 1.5 giờ sau giờ bắt đầu ca chiều
  // Cho phép check-in chiều sớm từ 12:00 để xử lý trường hợp check-out sáng muộn (12:07, 12:08...)
  const AFTERNOON_CHECKIN_EARLY = Math.max((morningEndMin || 720), timeStrToMinutes_("12:00")); // Từ 12:00 hoặc sau giờ kết thúc ca sáng
  const AFTERNOON_CHECKIN_LATE = (afternoonStartMin || 795) + 90; // 90 phút sau giờ bắt đầu ca chiều
  // Check-out chiều: từ 2 giờ sau check-in đến 2 giờ sau giờ kết thúc ca
  const AFTERNOON_CHECKOUT_EARLY = (afternoonStartMin || 795) + 120; // Ít nhất 2 giờ sau check-in
  const AFTERNOON_CHECKOUT_LATE = (afternoonEndMin || 1005) + 120; // 2 giờ sau giờ kết thúc ca chiều

  // Tính hạn check-in trễ dựa trên schedule của từng role (giờ bắt đầu ca + 30 phút)
  const MORNING_CHECKIN_LATE_THRESHOLD = morningStartMin !== null ? morningStartMin + 30 : timeStrToMinutes_("09:00"); // Giờ bắt đầu ca sáng + 30 phút
  const AFTERNOON_CHECKIN_LATE_THRESHOLD = afternoonStartMin !== null ? afternoonStartMin + 30 : timeStrToMinutes_("13:45"); // Giờ bắt đầu ca chiều + 30 phút

  // Khoảng cách tối thiểu giữa check-in và check-out (2 giờ)
  const MIN_SESSION_DURATION = 120; // 2 giờ

  let morningIn = null, morningOut = null;
  let afternoonIn = null, afternoonOut = null;

  const numTimes = timesSorted.length;
  // Không cần check registeredSessions - phân tích hoàn toàn dựa trên thời gian

  // Helper function: Kiểm tra xem một thời gian có thể là check-in sáng không
  const couldBeMorningCheckIn = (tMin) => {
    if (tMin === null) return false;
    return tMin >= MORNING_CHECKIN_EARLY && tMin <= MORNING_CHECKIN_LATE;
  };

  // Helper function: Kiểm tra xem một thời gian có thể là check-out sáng không
  const couldBeMorningCheckOut = (tMin) => {
    if (tMin === null) return false;
    return tMin >= MORNING_CHECKOUT_EARLY && tMin <= MORNING_CHECKOUT_LATE;
  };

  // Helper function: Kiểm tra xem một thời gian có thể là check-in chiều không
  const couldBeAfternoonCheckIn = (tMin) => {
    if (tMin === null) return false;
    return tMin >= AFTERNOON_CHECKIN_EARLY && tMin <= AFTERNOON_CHECKIN_LATE;
  };

  // Helper function: Kiểm tra xem một thời gian có thể là check-out chiều không
  const couldBeAfternoonCheckOut = (tMin) => {
    if (tMin === null) return false;
    return tMin >= AFTERNOON_CHECKOUT_EARLY && tMin <= AFTERNOON_CHECKOUT_LATE;
  };

  // XỬ LÝ THEO SỐ MỐC - Logic cải tiến để xử lý chính xác hơn
  // 1 ca = 2 mốc (in + out), 2 ca = 4 mốc (in sáng + out sáng + in chiều + out chiều)
  
  if (numTimes === 2) {
    // 2 mốc: Phân tích dựa trên thời gian để xác định là 1 ca (sáng hoặc chiều)
    const t1 = timesSorted[0];
    const t2 = timesSorted[1];
    const t1Min = timeStrToMinutes_(t1);
    const t2Min = timeStrToMinutes_(t2);

    // Ưu tiên phân tích dựa trên khoảng thời gian hợp lệ
    // Nếu cả 2 mốc đều nằm trong khoảng ca sáng -> 1 ca sáng (in + out)
    if (couldBeMorningCheckIn(t1Min) && couldBeMorningCheckOut(t2Min) &&
        t2Min > (t1Min || 0) + MIN_SESSION_DURATION) {
      morningIn = t1;
      morningOut = t2;
    }
    // Nếu cả 2 mốc đều nằm trong khoảng ca chiều -> 1 ca chiều (in + out)
    else if (couldBeAfternoonCheckIn(t1Min) && couldBeAfternoonCheckOut(t2Min) &&
             t2Min > (t1Min || 0) + MIN_SESSION_DURATION) {
      afternoonIn = t1;
      afternoonOut = t2;
    }
    // Trường hợp đặc biệt: mốc 1 có thể là check-in sáng, mốc 2 là check-out chiều
    // (thiếu check-out sáng và check-in chiều - làm cả 2 ca nhưng thiếu 2 mốc)
    else if (couldBeMorningCheckIn(t1Min) && couldBeAfternoonCheckOut(t2Min) &&
             t2Min > (t1Min || 0) + MIN_SESSION_DURATION * 2) {
      morningIn = t1;
      afternoonOut = t2;
    }
    // Trường hợp: mốc 1 là check-out sáng, mốc 2 là check-out chiều
    // (thiếu check-in sáng và check-in chiều)
    else if (couldBeMorningCheckOut(t1Min) && couldBeAfternoonCheckOut(t2Min) &&
             t2Min > (t1Min || 0)) {
      morningOut = t1;
      afternoonOut = t2;
    }
    // Trường hợp: mốc 1 là check-in sáng, mốc 2 là check-in chiều
    // (thiếu check-out sáng và check-out chiều)
    else if (couldBeMorningCheckIn(t1Min) && couldBeAfternoonCheckIn(t2Min) &&
             t2Min > (t1Min || 0) + MIN_SESSION_DURATION) {
      morningIn = t1;
      afternoonIn = t2;
    }
  } else if (numTimes === 4) {
    // 4 mốc: Làm đủ 2 ca - mốc 1=check-in sáng, mốc 2=check-out sáng, mốc 3=check-in chiều, mốc 4=check-out chiều
    const t1 = timesSorted[0];
    const t2 = timesSorted[1];
    const t3 = timesSorted[2];
    const t4 = timesSorted[3];
    const t1Min = timeStrToMinutes_(t1);
    const t2Min = timeStrToMinutes_(t2);
    const t3Min = timeStrToMinutes_(t3);
    const t4Min = timeStrToMinutes_(t4);

    // Xác định chính xác từng mốc dựa trên khoảng thời gian hợp lệ
    // Mốc 1: phải là check-in sáng
    if (couldBeMorningCheckIn(t1Min)) {
      morningIn = t1;
    }
    // Mốc 2: phải là check-out sáng (sau mốc 1 ít nhất 2 giờ)
    if (couldBeMorningCheckOut(t2Min) && t2Min > (t1Min || 0) + MIN_SESSION_DURATION) {
      morningOut = t2;
    }
    // Mốc 3: phải là check-in chiều (sau mốc 2 - check-out sáng)
    // Cho phép check-in chiều sớm từ 12:00 để xử lý trường hợp check-out sáng muộn (12:07, 12:08...)
    if (couldBeAfternoonCheckIn(t3Min) && t3Min > (t2Min || 0)) {
      afternoonIn = t3;
    }
    // Mốc 4: phải là check-out chiều (sau mốc 3 ít nhất 2 giờ)
    if (couldBeAfternoonCheckOut(t4Min) && t4Min > (t3Min || 0) + MIN_SESSION_DURATION) {
      afternoonOut = t4;
    }
  } else if (numTimes === 3) {
    // 3 mốc: Phân tích logic để xác định quên check-out sáng hoặc quên check-in chiều
    // Có thể là: in sáng + out sáng + in chiều (thiếu out chiều)
    // Hoặc: in sáng + in chiều + out chiều (thiếu out sáng)
    // Hoặc: in sáng + out sáng + out chiều (thiếu in chiều)
    const t1 = timesSorted[0];
    const t2 = timesSorted[1];
    const t3 = timesSorted[2];
    const t1Min = timeStrToMinutes_(t1);
    const t2Min = timeStrToMinutes_(t2);
    const t3Min = timeStrToMinutes_(t3);

    // Pattern 1: check-in sáng, check-out sáng, check-in/out chiều
    if (couldBeMorningCheckIn(t1Min)) {
      morningIn = t1;

      if (couldBeMorningCheckOut(t2Min) && t2Min > (t1Min || 0) + MIN_SESSION_DURATION) {
        // Mốc 2 là check-out sáng
        morningOut = t2;
        // Mốc 3 có thể là check-in hoặc check-out chiều
        if (couldBeAfternoonCheckIn(t3Min) && t3Min > (t2Min || morningEndMin || 0)) {
          afternoonIn = t3;
        } else if (couldBeAfternoonCheckOut(t3Min) && t3Min > (t2Min || morningEndMin || 0)) {
          afternoonOut = t3;
        }
      } else if (couldBeAfternoonCheckIn(t2Min) && t2Min > (t1Min || 0)) {
        // Mốc 2 là check-in chiều (quên check-out sáng)
        afternoonIn = t2;
        if (couldBeAfternoonCheckOut(t3Min) && t3Min > (t2Min || 0) + MIN_SESSION_DURATION) {
          afternoonOut = t3;
        }
      } else if (couldBeAfternoonCheckOut(t2Min) && t2Min > (t1Min || 0)) {
        // Mốc 2 là check-out chiều (quên check-out sáng và check-in chiều)
        afternoonOut = t2;
        // Mốc 3 có thể là check-in chiều muộn
        if (couldBeAfternoonCheckIn(t3Min) && t3Min < t2Min) {
          afternoonIn = t3;
        }
      }
    }
    // Pattern 2: check-in chiều, check-out chiều (quên cả ca sáng)
    else if (couldBeAfternoonCheckIn(t1Min)) {
      afternoonIn = t1;
      if (couldBeAfternoonCheckOut(t2Min) && t2Min > (t1Min || 0) + MIN_SESSION_DURATION) {
        afternoonOut = t2;
      } else if (couldBeAfternoonCheckOut(t3Min) && t3Min > (t1Min || 0) + MIN_SESSION_DURATION) {
        afternoonOut = t3;
      }
    }
    // Pattern 3: check-out sáng, check-in chiều, check-out chiều (quên check-in sáng)
    else if (couldBeMorningCheckOut(t1Min) && couldBeAfternoonCheckIn(t2Min) && couldBeAfternoonCheckOut(t3Min)) {
      morningOut = t1;
      afternoonIn = t2;
      if (t3Min > (t2Min || 0) + MIN_SESSION_DURATION) {
        afternoonOut = t3;
      }
    }
  } else if (numTimes === 1) {
    // 1 mốc: Phân tích dựa trên thời gian - chỉ có 1 mốc nên thiếu ít nhất 1 mốc
    const t1 = timesSorted[0];
    const t1Min = timeStrToMinutes_(t1);

    if (t1Min !== null) {
      // Phân tích dựa trên khoảng thời gian hợp lệ
      if (couldBeMorningCheckIn(t1Min)) {
        morningIn = t1;
      } else if (couldBeMorningCheckOut(t1Min)) {
        morningOut = t1;
      } else if (couldBeAfternoonCheckIn(t1Min)) {
        afternoonIn = t1;
      } else if (couldBeAfternoonCheckOut(t1Min)) {
        afternoonOut = t1;
      }
    }
  }

  // Tính late minutes: số phút trễ so với giờ bắt đầu ca
  let morningLateMinutes = 0;
  if (morningIn) {
    const morningInMin = timeStrToMinutes_(morningIn);
    if (morningInMin !== null && morningStartMin !== null) {
      if (morningInMin > morningStartMin) {
        morningLateMinutes = morningInMin - morningStartMin;
      }
    }
  }

  let afternoonLateMinutes = 0;
  if (afternoonIn) {
    const afternoonInMin = timeStrToMinutes_(afternoonIn);
    if (afternoonInMin !== null && afternoonStartMin !== null) {
      if (afternoonInMin > afternoonStartMin) {
        afternoonLateMinutes = afternoonInMin - afternoonStartMin;
      }
    }
  }

  // Phân loại times vào morning/afternoon để hiển thị
  // Dựa vào kết quả phân tích thực tế thay vì chỉ dựa vào cutoffMin = 12:00
  const cutoffMin = timeStrToMinutes_("12:00");
  const morningTimes = [];
  const afternoonTimes = [];
  
  // Xác định ranh giới giữa ca sáng và ca chiều dựa trên kết quả phân tích
  let morningEndBoundary = null;
  let afternoonStartBoundary = null;
  
  if (morningOut) {
    const morningOutMin = timeStrToMinutes_(morningOut);
    if (morningOutMin !== null) {
      morningEndBoundary = morningOutMin; // Tất cả mốc <= morningOut thuộc ca sáng
    }
  }
  
  if (afternoonIn) {
    const afternoonInMin = timeStrToMinutes_(afternoonIn);
    if (afternoonInMin !== null) {
      afternoonStartBoundary = afternoonInMin; // Tất cả mốc >= afternoonIn thuộc ca chiều
    }
  }
  
  // Phân loại từng mốc thời gian
  for (const t of timesSorted) {
    const tMin = timeStrToMinutes_(t);
    if (tMin === null) continue;
    
    // Nếu có ranh giới rõ ràng từ kết quả phân tích, dùng ranh giới đó
    if (morningEndBoundary !== null && tMin <= morningEndBoundary) {
      morningTimes.push(t);
    } else if (afternoonStartBoundary !== null && tMin >= afternoonStartBoundary) {
      afternoonTimes.push(t);
    } else {
      // Fallback: dựa vào cutoffMin = 12:00
      if (tMin < cutoffMin) {
        morningTimes.push(t);
      } else {
        afternoonTimes.push(t);
      }
    }
  }

  // Tạo kết quả
  out['morning'] = {
    times: morningTimes,
    in: morningIn,
    out: morningOut,
    // missingIn: thiếu check-in ca sáng (có thể có check-out nhưng không có check-in)
    missingIn: morningIn === null && (morningOut !== null || morningTimes.length > 0),
    // missingOut: có check-in nhưng thiếu check-out
    missingOut: morningIn !== null && morningOut === null,
    lateMinutes: morningLateMinutes,
    earlyMinutes: 0
  };

  out['afternoon'] = {
    times: afternoonTimes,
    in: afternoonIn,
    out: afternoonOut,
    // missingIn: thiếu check-in ca chiều (có thể có check-out nhưng không có check-in)
    missingIn: afternoonIn === null && (afternoonOut !== null || afternoonTimes.length > 0),
    // missingOut: có check-in nhưng thiếu check-out
    missingOut: afternoonIn !== null && afternoonOut === null,
    lateMinutes: afternoonLateMinutes,
    earlyMinutes: 0
  };

  return out;
}

// --- Helper: generate notes for sessions ---
function generateNotesForSessions_(emp, dayStr, sessionsMap, schedule, cfg, month) {
  const notes = [];
  let lateCount = 0, offForgotCount = 0;
  // Bỏ onlForgotCount - không sử dụng nữa

  // get cutoffMin to decide off vs onl categorization
  const cutoffMin = timeStrToMinutes_(schedule.cutoff || cfg.cutoff || SPECIAL_SCHEDULES.default.cutoff);

  for (const sessionName of Object.keys(sessionsMap)) {
    const s = sessionsMap[sessionName];
    // human-friendly name mapping
    const n = sessionName.toLowerCase();
    let human = '';
    if (n.includes('sang') || n.includes('s1')) human = 'ca sáng';
    else if (n.includes('s3') || n.includes('chieu_2') || n.includes('s3')) human = 'ca tối';
    else human = 'ca chiều';

    if (s.in == null) {
      notes.push(`- Quên check in ${human} ${dayStr}/${month}`);
      // Tất cả lỗi quên check-in đều đếm vào offForgotCount (cột U)
      offForgotCount++;
    }

    // Report missing checkout - no late/early reporting, just check if checkout exists
    if (s.out == null && s.in != null) {
      notes.push(`- Quên check out ${human} ${dayStr}/${month}`);
      // Tất cả lỗi quên check-out đều đếm vào offForgotCount (cột U)
      offForgotCount++;
    }

    // Note: "Trễ" (late) and "Về sớm" (early) logic removed - only check for missing check-in/check-out
  }

  return { notes, lateCount, offForgotCount };
}

// --- Helper: prepare per-row attendance changes (updated to use schedules) ---
// NOTE: Hàm này cần schedule để chỉ kiểm tra các ca đã đăng ký
// Nếu không có schedule, sẽ không kiểm tra gì (tránh báo lỗi sai)
function prepareAttendanceChanges_(timesByEmpDay, masterInfo, cfg, month, scheduleMap) {
  // returns { changes: Map<r0, {notes...}>, problematicCells: [{r0,c0,emp,dayStr,timesCount}] }
  const changes = new Map();
  const problematicCells = [];
  const threshold = cfg.maxTimesThreshold || 4; // default max allowed times per cell

  // Nếu không có scheduleMap, không thể kiểm tra (cần đăng ký ca để đối chiếu)
  if (!scheduleMap || scheduleMap.size === 0) {
    Logger.log('WARNING: prepareAttendanceChanges_ called without scheduleMap - cannot check attendance without schedule');
    return { changes, problematicCells };
  }

  // helper to convert session name to human
  const humanForSession = (sess) => {
    if (!sess) return 'ca';
    if (sess === 'morning') return 'ca sáng';
    if (sess === 'afternoon') return 'ca chiều';
    if (sess === 'evening') return 'ca tối';
    return 'ca';
  };

  // Chỉ kiểm tra các nhân viên có đăng ký ca trong schedule
  for (const [emp, dayMap] of scheduleMap.entries()) {
    const r1 = masterInfo.empToRow.get(emp);
    if (!r1) continue; // employee not in master attendance sheet
    const r0 = r1 - 1;

    let notesForDetail = [];
    let lateCount = 0, offForgotCount = 0;

    const role = masterInfo.empToRole ? masterInfo.empToRole.get(emp) : undefined;
    const schedule = getEmployeeSchedule_(emp, cfg, role);

    for (const [dayStr, sessSet] of dayMap.entries()) {
      // get times from raw map first
      const rawDayMap = timesByEmpDay.get(emp);
      const timesArr = rawDayMap && rawDayMap.get(dayStr) ? Array.from(rawDayMap.get(dayStr)) : [];

      // if no raw times, check masterInfo dayBlock cell (maybe times already merged there)
      if ((!timesArr || timesArr.length === 0) && masterInfo) {
        const col1 = masterInfo.colByDay.get(dayStr);
        if (col1) {
          const c0 = col1 - masterInfo.minDayCol;
          const existing = masterInfo.dayBlock[r0][c0];
          const extracted = extractTimesFromCell_(existing);
          if (extracted && extracted.length) {
            timesArr.push(...extracted);
          }
        }
      }

      if (!timesArr || timesArr.length === 0) {
        // no times: missing check-ins for all scheduled sessions
        for (const sess of Array.from(sessSet)) {
          const human = humanForSession(sess);
          notesForDetail.push(`- Quên check in ${human} ${dayStr}/${month}`);
          offForgotCount++;
        }
      } else {
        // Check if too many check-ins/outs in a single cell
        if (timesArr && timesArr.length > threshold) {
          const col1 = masterInfo.colByDay.get(dayStr);
          if (col1) {
            const c0 = col1 - masterInfo.minDayCol;
            if (c0 >= 0 && c0 < masterInfo.dayColsCount) {
              problematicCells.push({ r0, c0, emp, dayStr, timesCount: timesArr.length });
              Logger.log(`SKIP cell r=${r0 + 1} c=${c0 + masterInfo.minDayCol} emp=${emp} day=${dayStr} times=${timesArr.length} (> ${threshold})`);
            }
          }
          continue; // Skip processing this cell
        }

        // there are times -> compute sessions and check against registered sessions
        const sessionsMap = computeSessionsBySchedule_(timesArr, schedule, sessSet);

        // Chỉ kiểm tra các ca đã đăng ký trong schedule
        for (const registeredSess of Array.from(sessSet)) {
          // Map registered session (morning/afternoon/evening) to session name in sessionsMap
          let sessionFound = null;
          for (const [sessionName, session] of Object.entries(sessionsMap)) {
            const sessionNameLower = sessionName.toLowerCase();
            const registeredLower = String(registeredSess).toLowerCase();

            if (sessionNameLower === registeredLower ||
              (registeredLower === 'morning' && (sessionNameLower.includes('sang') || sessionNameLower === 'morning')) ||
              (registeredLower === 'afternoon' && (sessionNameLower.includes('chieu') || sessionNameLower === 'afternoon')) ||
              (registeredLower === 'evening' && (sessionNameLower.includes('toi') || sessionNameLower === 'evening'))) {
              sessionFound = session;
              break;
            }
          }

          const human = humanForSession(registeredSess);

          // Nếu không tìm thấy session trong chấm công = quên check-in
          if (!sessionFound || sessionFound.in == null) {
            notesForDetail.push(`- Quên check in ${human} ${dayStr}/${month}`);
            offForgotCount++;
          } else if (sessionFound.out == null) {
            // Có check-in nhưng quên check-out
            notesForDetail.push(`- Quên check out ${human} ${dayStr}/${month}`);
            offForgotCount++;
          }
        }
      }
    }

    if (notesForDetail.length) changes.set(r0, { notes: notesForDetail, lateCount, offForgotCount });
  }

  return { changes, problematicCells };
}

/**
 * Helper: phát hiện lỗi QUÊN CHECK IN/OUT cho 1 session (simple mode)
 * Trả về: { notes: string[], offForgotDelta: number }
 * Đồng bộ với highlight: báo lỗi khi có bằng chứng session (có times hoặc có in/out).
 */
function handleMissingCheckInOutSimple_(session, human, dayStr, month, scheduleTemplate, sessionName, totalTimesInDay) {
  const notes = [];
  let offForgotDelta = 0;

  if (!session) return { notes, offForgotDelta };

  // Có bằng chứng session: có ít nhất 1 mốc thời gian HOẶC có in/out (để không sót quên check in ca chiều khi chỉ có out)
  const hasTimes = Array.isArray(session.times) && session.times.length > 0;
  const hasInOrOut = session.in != null || session.out != null;
  if (!hasTimes && !hasInOrOut) {
    return { notes, offForgotDelta };
  }

  // ===== TRƯỜNG HỢP ĐẶC BIỆT: ĐỦ ≥4 MỐC TRONG NGÀY NHƯNG BỊ COI LÀ QUÊN CHECK IN CA =====
  // Điều kiện:
  // - Cả ngày có đủ ≥ 4 mốc (điển hình 4 mốc: 2 sáng, 2 chiều)
  // - Session hiện tại bị đánh dấu missingIn & in === null (logic cũ coi là "Quên check in")
  // → Chuyển thành "Check in trễ từ 30 phút trở lên" (ghi ở cột W)
  if (
    typeof totalTimesInDay === 'number' &&
    totalTimesInDay >= 4 &&
    session.missingIn &&
    session.in === null &&
    Array.isArray(session.times) &&
    session.times.length > 0
  ) {
    // Xác định giờ bắt đầu ca theo scheduleTemplate
    let startStr = null;
    if (sessionName === 'morning') {
      startStr = scheduleTemplate && scheduleTemplate.morning && scheduleTemplate.morning.start;
    } else if (sessionName === 'afternoon') {
      startStr = scheduleTemplate && scheduleTemplate.afternoon && scheduleTemplate.afternoon.start;
    }

    const firstTimeStr = session.times
      .slice()
      .sort((a, b) => (timeStrToMinutes_(a) || 0) - (timeStrToMinutes_(b) || 0))[0];

    const startMin = startStr != null ? timeStrToMinutes_(startStr) : null;
    const firstMin = firstTimeStr != null ? timeStrToMinutes_(firstTimeStr) : null;

    if (startMin != null && firstMin != null) {
      const diff = firstMin - startMin;
      const threshold = 30;

      if (diff >= threshold) {
        // Ghi lỗi "trễ từ 30 phút trở lên" nhưng vẫn tính vào nhóm missing (cột W)
        notes.push(`- Check in trễ từ 30 phút trở lên (${diff} phút) ${human} ${dayStr}/${month}`);
        offForgotDelta++;
        return { notes, offForgotDelta };
      }
    }
    // Nếu không tính được diff hợp lệ → rơi xuống logic cũ phía dưới
  }

  // ===== LOGIC CŨ (giữ nguyên cho mọi trường hợp còn lại) =====

  // Quên check-in: thiếu in (có out hoặc có times trong ca đó)
  if (session.missingIn && session.in === null) {
    notes.push(`- Quên check in ${human} ${dayStr}/${month}`);
    offForgotDelta++;
  }
  // Quên check-out: có in nhưng thiếu out
  else if (session.missingOut && session.in != null && session.out === null) {
    notes.push(`- Quên check out ${human} ${dayStr}/${month}`);
    offForgotDelta++;
  }

  return { notes, offForgotDelta };
}

/**
 * Helper: phát hiện lỗi TRỄ check-in cho 1 session (simple mode)
 * - session.lateMinutes đã là số phút trễ so với giờ bắt đầu ca
 * - Trễ > 30 phút: LUÔN quy thành lỗi QUÊN CHECK IN (note ghi cột W, không ghi cột S), cộng offForgotDelta
 * - Trễ <= 30 phút: ghi "trễ dưới 30 phút" vào cột S, cộng lateDelta (cột Q)
 * Trả về: { notes: string[], lateDelta: number, offForgotDelta: number }
 */
function handleLateSimple_(session, human, dayStr, month, masterInfo, r0, emp, sessionName, problematicCells) {
  const notes = [];
  let lateDelta = 0;
  let offForgotDelta = 0;

  // Xét TRỄ khi có check-in và lateMinutes > 0 (đồng bộ với highlight: không bắt buộc có session.out).
  // Trường hợp chỉ có check-in (quên check-out) vẫn ghi note trễ nếu check-in trễ, để khớp với ô đã tô đỏ.
  // Dùng Number() để tránh sót khi lateMinutes là số thực/string từ computeSessionsBySchedule_
  const lateMinNum = Number(session && session.lateMinutes);
  if (session && session.in && !isNaN(lateMinNum) && lateMinNum > 0) {
    const lateMinutes = Math.round(lateMinNum);
    const threshold = 30;

    // YÊU CẦU: Trễ > 30 phút LUÔN quy thành lỗi QUÊN CHECK IN (ghi cột W), KHÔNG ghi cột S (lỗi trễ).
    // Trễ <= 30 phút (bao gồm cả = 30 phút) là lỗi TRỄ, ghi cột S.
    if (lateMinutes > threshold) {
      // NOTE: Dòng này được ghi vào cột W (nhóm lỗi "missing"), không ghi vào cột S.
      notes.push(`- Check in trễ từ 30 phút trở lên (${lateMinutes} phút) ${human} ${dayStr}/${month}`);
      offForgotDelta++;

      const col1 = masterInfo.colByDay.get(dayStr);
      if (col1) {
        const c0 = col1 - masterInfo.minDayCol;
        if (c0 >= 0 && c0 < masterInfo.dayColsCount) {
          problematicCells.push({
            r0,
            c0,
            emp,
            dayStr,
            type: 'missing_in_over_30',
            sessionName: sessionName,
            lateMinutes: session.lateMinutes,
            checkInTime: session.in
          });
        }
      }
    } else {
      // Trễ <= 30 phút (bao gồm cả = 30 phút): vẫn là lỗi TRỄ, ghi cột S và cộng lateCount (cột Q)
      notes.push(`- Check in trễ dưới 30 phút (${lateMinutes} phút) ${human} ${dayStr}/${month}`);
      lateDelta++;
      // Ghi vào problematicCells để applyAttendance có thể đồng bộ note nếu bị sót (ô tô đỏ nhưng không có note)
      if (problematicCells && masterInfo) {
        const col1 = masterInfo.colByDay.get(dayStr);
        if (col1) {
          const c0 = col1 - masterInfo.minDayCol;
          if (c0 >= 0 && c0 < masterInfo.dayColsCount) {
            problematicCells.push({
              r0,
              c0,
              emp,
              dayStr,
              type: 'late',
              sessionName: sessionName,
              lateMinutes: session.lateMinutes,
              checkInTime: session.in
            });
          }
        }
      }
    }
  }

  return { notes, lateDelta, offForgotDelta };
}

/**
 * Đếm số ca làm trong tháng cho một nhân viên từ dữ liệu vân tay.
 * Dùng cho cột BU (TỔNG ca off vân tay) = số lượng ca làm trong tháng.
 * Mỗi ca được tính nếu có bằng chứng đi làm: có check-in HOẶC check-out HOẶC có ít nhất một mốc thời gian trong ca
 * (quên check in/out vẫn tính 1 ca vì họ vẫn có đi làm).
 * @param {number} r0 - Row index (0-based) của nhân viên trong master sheet
 * @param {Object} masterInfo - Thông tin master sheet (đã dùng fingerprint range 36-66)
 * @param {Object} schedule - Lịch làm việc của nhân viên
 * @param {Object} cfg - Config attendance
 * @return {number} - Tổng số ca làm trong tháng (sáng + chiều)
 */
function countCompleteSessionsInMonth_(r0, masterInfo, schedule, cfg) {
  let total = 0;
  for (let day = 1; day <= 31; day++) {
    const dayStr = String(day);
    const col1 = masterInfo.colByDay.get(dayStr);
    if (!col1) continue;
    const c0 = col1 - masterInfo.minDayCol;
    if (c0 < 0 || c0 >= masterInfo.dayColsCount) continue;

    const existing = masterInfo.dayBlock[r0][c0];
    let times = extractTimesFromCell_(existing);
    if (!times || !times.length) times = extractTimes_(String(existing || ''));
    if (!times || times.length === 0) continue;

    const sessionsMap = computeSessionsBySchedule_(times, schedule, null);
    for (const [sessionName, session] of Object.entries(sessionsMap)) {
      if (sessionName === '_problematic' || sessionName === '_timesCount') continue;
      if (!session) continue;
      // Có bằng chứng ca: đủ in+out, hoặc quên check in (có out), hoặc quên check out (có in), hoặc có times trong ca
      const hasInOrOut = session.in != null || session.out != null;
      const hasTimes = Array.isArray(session.times) && session.times.length > 0;
      if (hasInOrOut || hasTimes) total++;
    }
  }
  return total;
}

function findHeaderCols_(headerRow) {
  const map = {};
  const norm = (s) => normalize_(s || "");
  const headers = headerRow.map(h => ({ raw: h, n: norm(h) }));

  // detail columns (chi tiet) -> collect in order
  const detailIdx = [];
  headers.forEach((h, idx) => { if (h.n.includes("chi tiet")) detailIdx.push(idx + 1); });
  
  // Tìm cột S cụ thể (cột 19) để ghi note TRỄ
  map.detail2Col = null;
  // Ưu tiên 1: Luôn dùng cột S (19) nếu tồn tại (bất kể header là gì)
  if (headers.length >= 19) {
    map.detail2Col = 19; // Cột S - luôn dùng để ghi note TRỄ
  }
  // Ưu tiên 2: Nếu cột S không tồn tại, tìm cột có "Chi tiết(2)" hoặc "Chi tiết (2)"
  if (!map.detail2Col) {
    headers.forEach((h, idx) => {
      if ((h.n.includes("chi tiet") && (h.n.includes("2") || h.raw.includes("(2)"))) ||
          (h.raw.includes("Chi tiết(2)") || h.raw.includes("Chi tiết (2)"))) {
        map.detail2Col = idx + 1;
      }
    });
  }
  // Fallback: Nếu không tìm thấy, dùng cột "Chi tiết" đầu tiên
  if (!map.detail2Col && detailIdx.length) {
    map.detail2Col = detailIdx[0];
  }
  if (detailIdx.length > 1) map.detail3Col = detailIdx[1];
  // lateNoteCol: ưu tiên Chi tiết(2) (thường là cột S) để ghi note TRỄ
  map.lateNoteCol = map.detail2Col || null;

  // Cột Q (17): số lượng lỗi TRỄ
  // YÊU CẦU: luôn hiển thị số lượng lỗi trễ tại cột Q => ưu tiên tuyệt đối nếu tồn tại.
  map.totalLateCol = null;
  if (headers.length >= 17) {
    map.totalLateCol = 17; // Column Q
  }

  // Tìm cột W cụ thể - cột có header "CHI TIẾT (3)" hoặc "chi tiet" và số "3"
  map.noteCol = null;
  headers.forEach((h, idx) => {
    if (h.n.includes("chi tiet") && (h.n.includes("3") || h.raw.includes("(3)") || h.raw.includes("3"))) {
      map.noteCol = idx + 1;
    }
  });
  // Nếu không tìm thấy, thử tìm cột W (cột 23) trực tiếp
  if (!map.noteCol && headers.length >= 23) {
    const wHeader = headers[22]; // index 22 = column 23 (W)
    if (wHeader && wHeader.n.includes("chi tiet")) {
      map.noteCol = 23;
    }
  }
  // Fallback: nếu vẫn không tìm thấy, dùng cột chi tiết cuối cùng
  if (!map.noteCol && detailIdx.length > 0) {
    map.noteCol = detailIdx[detailIdx.length - 1];
  }

  // Tìm cột AA "Chi tiết(4)" - cột có header "CHI TIẾT (4)" hoặc "chi tiet" và số "4"
  map.detail4Col = null;
  headers.forEach((h, idx) => {
    if (h.n.includes("chi tiet") && (h.n.includes("4") || h.raw.includes("(4)") || h.raw.includes("4"))) {
      map.detail4Col = idx + 1;
    }
  });
  // Nếu không tìm thấy, thử tìm cột AA (cột 27) trực tiếp
  if (!map.detail4Col && headers.length >= 27) {
    const aaHeader = headers[26]; // index 26 = column 27 (AA)
    if (aaHeader && aaHeader.n.includes("chi tiet")) {
      map.detail4Col = 27;
    }
  }

  // Cột BU (73) - TỔNG ca off vân tay = số ca làm trong tháng (đếm từ dữ liệu vân tay)
  map.totalCaOffVanTayCol = null;
  if (headers.length >= 73) map.totalCaOffVanTayCol = 73; // BU
  headers.forEach((h, idx) => {
    if ((h.n.includes("tong") || h.n.includes("tổng")) && h.n.includes("ca") && (h.n.includes("van tay") || h.n.includes("vân tay"))) {
      map.totalCaOffVanTayCol = idx + 1;
    }
  });

  // totals and flags
  headers.forEach((h, idx) => {
    const i = idx + 1;
    // totalLateCol đã ưu tiên cố định cột Q ở trên; chỉ fallback nếu sheet thiếu cột Q
    if (!map.totalLateCol && h.n.includes("tong tre")) map.totalLateCol = i;
    // Tìm cột U (21) cho OFF QUÊN CHECK IN/OUT/TRỄ >=30' - ưu tiên cột U cố định
    if (!map.offForgotCol && headers.length >= 21) {
      map.offForgotCol = 21; // Cột U - luôn dùng để đếm lỗi OFF
    }
    if (!map.offForgotCol && h.n.includes("off quen")) map.offForgotCol = i;
    // Tìm cột P (16) cho TRỄ OFF NGOÀI (nhập tay) - ưu tiên cột P cố định
    if (!map.offLateCol && headers.length >= 16) {
      map.offLateCol = 16; // Cột P - luôn dùng để đếm lỗi trễ OFF ngoài
    }
    if (!map.offLateCol && (h.n.includes("tre off ngoai") || h.n.includes("trễ off ngoài") || (h.n.includes("tre") && h.n.includes("off") && h.n.includes("ngoai")))) {
      map.offLateCol = i;
    }
    // Tìm cột V (22) cho ONL QUÊN CHECK IN/OUT/TRỄ >=30' - ưu tiên cột V cố định
    if (!map.onlForgotCol && headers.length >= 22) {
      map.onlForgotCol = 22; // Cột V - luôn dùng để đếm lỗi ONL
    }
    if (!map.onlForgotCol && h.n.includes("onl") && (h.n.includes("quen") || h.n.includes("tre") || h.n.includes("trễ"))) map.onlForgotCol = i;
    if (!map.moneyCol && h.n.includes("tien phat")) map.moneyCol = i;
    if (!map.vangOffCol && (h.n.includes("vang off") || h.n.includes("vắng off") || h.n.includes("vang") && h.n.includes("off"))) map.vangOffCol = i;
  });

  return map;
}

// Các hàm schedule-aware đã được xóa - chỉ sử dụng applyAttendance() với logic đơn giản (không cần schedule)

/**
 * Highlight problematic cells (with too many check-ins and late check-ins) for review. 
 * Highlights ALL problematic cells in the sheet by checking ALL employees.
 * Checks for:
 * 1. Cells with >4 time stamps (màu đỏ nhạt)
 * 2. Cells with late check-in (màu đỏ) - tính toán chính xác theo role (quản lý vs nhân viên bình thường)
 */
function highlightProblematicCells() {
  const MASTER_FILE_ID = "1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4";
  const MASTER_SHEET_NAME = "Chấm công th12/2025";
  const RAW_FILE_ID = "1ed1IK4X1bQxhBoz4tjUKEypIv6cipNKsUCcXPKjqy8o";
  const RAW_SHEETS = ["L4_HH"];
  const cfg = { morningStart: "08:30", afternoonStart: "13:15", cutoff: "12:00", lateThreshold: 30, maxTimesThreshold: 4 };

  // Dữ liệu vân tay nằm ở cột AJ (36) đến BN (66), nhân viên từ hàng 2. Chỉ lấy cột ngày trong khối này để tô đúng ô.
  const FINGERPRINT_DAY_COL_MIN = 36;  // AJ
  const FINGERPRINT_DAY_COL_MAX = 66;  // BN
  Logger.log("1) Loading data...");
  const timesByEmpDay = buildTimesFromRawSheets_(RAW_FILE_ID, RAW_SHEETS);
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, 2, 1, FINGERPRINT_DAY_COL_MIN, FINGERPRINT_DAY_COL_MAX);
  const month = parseMonthFromSheetName_(MASTER_SHEET_NAME) || 12;

  const problematicCells = [];
  const threshold = cfg.maxTimesThreshold || 4;

  Logger.log("2) Checking ALL employees in master sheet...");

  // Duyệt TẤT CẢ nhân viên trong master sheet, không chỉ những người có đăng ký OFF
  for (const [emp, r1] of masterInfo.empToRow.entries()) {
    const r0 = r1 - 1;
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(emp) : undefined;
    const schedule = getEmployeeSchedule_(emp, cfg, role);

    // Duyệt tất cả các ngày trong tháng (1-31)
    for (let day = 1; day <= 31; day++) {
      const dayStr = String(day);
      const col1 = masterInfo.colByDay.get(dayStr);
      if (!col1) continue; // Ngày này không có trong header

      const c0 = col1 - masterInfo.minDayCol;
      if (c0 < 0 || c0 >= masterInfo.dayColsCount) continue;

      // Lấy dữ liệu chấm công:
      // - ƯU TIÊN: dữ liệu hiện tại trong master sheet (đã chỉnh sửa thủ công)
      // - FALLBACK: raw map (chỉ khi ô master trống hoàn toàn)
      let timesArr = [];
      const existing = masterInfo.dayBlock[r0][c0];
      const masterHasAnyValue = (existing !== null && existing !== undefined && String(existing).trim() !== '');
      const extracted = extractTimesFromCell_(existing);
      if (extracted && extracted.length) {
        timesArr = extracted;
      } else if (!masterHasAnyValue) {
        const rawDayMap = timesByEmpDay.get(emp);
        if (rawDayMap && rawDayMap.get(dayStr)) {
          timesArr = Array.from(rawDayMap.get(dayStr));
        }
      }

      if (!timesArr || timesArr.length === 0) continue; // Không có dữ liệu, bỏ qua

      // Kiểm tra 1: >4 mốc thời gian
      if (timesArr.length > threshold) {
        problematicCells.push({
          r0,
          c0,
          emp,
          dayStr,
          type: 'tooManyTimes',
          timesCount: timesArr.length
        });
        Logger.log(`FOUND >4 times: emp=${emp} day=${dayStr} times=${timesArr.length}`);
        continue; // Skip kiểm tra check-in trễ nếu >4 mốc
      }

      // Kiểm tra 2: Check-in trễ
      // Tính sessions để kiểm tra lateMinutes
      const sessionsMap = computeSessionsBySchedule_(timesArr, schedule, null);

      for (const [sessionName, session] of Object.entries(sessionsMap)) {
        // Bỏ qua các session không hợp lệ
        if (sessionName === '_problematic' || sessionName === '_timesCount') continue;

        // Kiểm tra nếu có check-in và có lateMinutes > 0
        if (session.in && session.lateMinutes && session.lateMinutes > 0) {
          problematicCells.push({
            r0,
            c0,
            emp,
            dayStr,
            type: 'late',
            sessionName: sessionName,
            lateMinutes: session.lateMinutes,
            checkInTime: session.in
          });
          Logger.log(`FOUND late check-in: emp=${emp} day=${dayStr} session=${sessionName} late=${session.lateMinutes}min checkIn=${session.in}`);
        }
      }
    }
  }

  Logger.log(`3) Found ${problematicCells.length} problematic cells total`);

  if (!problematicCells.length) {
    Logger.log('No problematic cells found');
    return;
  }

  // Phân loại problematic cells
  const tooManyTimesCells = problematicCells.filter(p => p.type === 'tooManyTimes');
  const lateCheckInCells = problematicCells.filter(p => p.type === 'late');

  Logger.log(`4) Highlighting: ${tooManyTimesCells.length} cells with >4 times, ${lateCheckInCells.length} cells with late check-in`);

  // Tối ưu: Batch highlight thay vì highlight từng cell để tránh timeout
  // Gom các cells cùng màu lại và highlight cùng lúc
  
  // Highlight cells với >4 mốc thời gian (màu đỏ nhạt) - batch mode
  if (tooManyTimesCells.length > 0) {
    try {
      // Gom các ranges lại nếu có thể, hoặc highlight từng batch nhỏ
      const BATCH_SIZE = 20; // Highlight 20 cells mỗi lần
      for (let i = 0; i < tooManyTimesCells.length; i += BATCH_SIZE) {
        const batch = tooManyTimesCells.slice(i, i + BATCH_SIZE);
        const ranges = batch.map(p => {
          const rowNum = p.r0 + 1;
          const colNum = masterInfo.minDayCol + p.c0;
          return masterSh.getRange(rowNum, colNum);
        });
        
        // Highlight tất cả cells trong batch cùng lúc
        ranges.forEach((range, idx) => {
          try {
            range.setBackground('#ffcccc');
            const p = batch[idx];
            Logger.log(`HIGHLIGHTED (>4 times) row=${p.r0 + 1} col=${masterInfo.minDayCol + p.c0} emp=${p.emp} day=${p.dayStr} times=${p.timesCount}`);
          } catch (e) {
            Logger.log(`Failed to highlight cell in batch: ${e.message}`);
          }
        });
        
        // Flush sau mỗi batch để đảm bảo được ghi
        SpreadsheetApp.flush();
        
        // Nghỉ ngắn giữa các batch để tránh rate limit
        if (i + BATCH_SIZE < tooManyTimesCells.length) {
          Utilities.sleep(50); // 50ms delay
        }
      }
    } catch (e) {
      Logger.log(`ERROR highlighting >4 times cells: ${e.message}`);
    }
  }

  // Highlight cells với check-in trễ (màu đỏ) - batch mode
  if (lateCheckInCells.length > 0) {
    try {
      const BATCH_SIZE = 20; // Highlight 20 cells mỗi lần
      for (let i = 0; i < lateCheckInCells.length; i += BATCH_SIZE) {
        const batch = lateCheckInCells.slice(i, i + BATCH_SIZE);
        const ranges = batch.map(p => {
          const rowNum = p.r0 + 1;
          const colNum = masterInfo.minDayCol + p.c0;
          return masterSh.getRange(rowNum, colNum);
        });
        
        // Highlight tất cả cells trong batch cùng lúc
        ranges.forEach((range, idx) => {
          try {
            range.setBackground('#ff0000');
            const p = batch[idx];
            Logger.log(`HIGHLIGHTED (late check-in) row=${p.r0 + 1} col=${masterInfo.minDayCol + p.c0} emp=${p.emp} day=${p.dayStr} session=${p.sessionName} late=${p.lateMinutes}min checkIn=${p.checkInTime}`);
          } catch (e) {
            Logger.log(`Failed to highlight cell in batch: ${e.message}`);
          }
        });
        
        // Flush sau mỗi batch
        SpreadsheetApp.flush();
        
        // Nghỉ ngắn giữa các batch
        if (i + BATCH_SIZE < lateCheckInCells.length) {
          Utilities.sleep(50); // 50ms delay
        }
      }
    } catch (e) {
      Logger.log(`ERROR highlighting late check-in cells: ${e.message}`);
    }
  }

  Logger.log(`5) Completed highlighting ${problematicCells.length} problematic cells (${tooManyTimesCells.length} >4 times, ${lateCheckInCells.length} late check-in)`);

  // Final flush để đảm bảo tất cả highlight được ghi
  SpreadsheetApp.flush();
  
  // Hiển thị thông báo bằng toast (không chặn execution)
  try {
    const message = `Đã highlight ${problematicCells.length} ô:\n- ${tooManyTimesCells.length} ô >4 mốc\n- ${lateCheckInCells.length} ô trễ`;
    masterSh.getRange(1, 1).setValue(masterSh.getRange(1, 1).getValue()); // Trigger refresh
    SpreadsheetApp.getActiveSpreadsheet().toast(message, "Hoàn thành", 5);
    Logger.log(`Toast notification: ${message}`);
  } catch (e) {
    Logger.log(`Notification skipped. Completed: ${problematicCells.length} cells highlighted.`);
  }
}

// Legacy commit wrapper removed — use `applyAttendance({ useSchedule: false, dryRun: false })` instead.

// -------------------------------------------------------------------------------------

/**
 * Debug helper: inspect parsing and session assignment for a specific employee/day
 * Call: debugAttendance('MH0010', '2')
 */
function debugAttendance(empId, dayStr) {
  const RAW_FILE_ID = "1ed1IK4X1bQxhBoz4tjUKEypIv6cipNKsUCcXPKjqy8o";
  const RAW_SHEETS = ["L4_HH"];
  const MASTER_FILE_ID = "1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4";
  const MASTER_SHEET_NAME = "Chấm công th12/2025";

  Logger.log(`DEBUG: Inspecting ${empId} day=${dayStr}`);

  // raw times
  const timesByEmpDay = buildTimesFromRawSheets_(RAW_FILE_ID, RAW_SHEETS);
  const empMap = timesByEmpDay.get(String(empId).toUpperCase());
  Logger.log(`  raw map present=${!!empMap}`);
  if (empMap) {
    const rawSet = empMap.get(String(dayStr));
    Logger.log(`  raw times for day: ${rawSet ? Array.from(rawSet).join(',') : '<none>'}`);
  }

  // master cell
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  const row = masterInfo.empToRow.get(String(empId).toUpperCase());
  const col = masterInfo.colByDay.get(String(dayStr));
  if (!row || !col) { Logger.log('  Not found in master (row or col missing)'); return; }
  const r0 = row - 1; const c0 = col - masterInfo.minDayCol;
  const cellVal = masterInfo.dayBlock[r0][c0];
  Logger.log('  master cell raw value: ' + String(cellVal || '<empty>'));

  const parsed = extractTimesFromCell_(cellVal);
  Logger.log('  extractTimesFromCell_ => [' + parsed.join(',') + ']');

  // fallback: also run extractTimes_ on string (in case other formats)
  const fallback = extractTimes_(String(cellVal || ''));
  Logger.log('  extractTimes_ (fallback regex) => [' + fallback.join(',') + ']');

  const times = (parsed.length ? parsed : fallback);
  Logger.log('  final times used: [' + times.join(',') + ']');

  // schedule & sessions
  const cfg = { morningStart: "08:30", afternoonStart: "13:15", cutoff: "12:00", lateThreshold: 30 };
  const role = masterInfo.empToRole ? masterInfo.empToRole.get(empId.toUpperCase()) : undefined;
  Logger.log('  role (from sheet): ' + String(role || '<none>'));

  const schedule = getEmployeeSchedule_(empId, cfg, role);
  Logger.log('  schedule: ' + JSON.stringify(schedule));

  const sessionsMap = computeSessionsBySchedule_(times, schedule, null);
  Logger.log('  computed sessions: ' + JSON.stringify(sessionsMap));

  // detailed breakdown
  for (const k of Object.keys(sessionsMap)) {
    const s = sessionsMap[k];
    Logger.log(`    session ${k}: times=[${(s.times || []).join(',')}] in=${s.in} out=${s.out} missingIn=${s.missingIn} missingOut=${s.missingOut} late=${s.lateMinutes} early=${s.earlyMinutes}`);
  }

  Logger.log('DEBUG: finished');
}

// ----------------------- Schedule registration parsing & integration -----------------------

/**
 * Build a name-to-employee-code map from MASTER_DATA_EMPLOYEES file
 * Expects a sheet with columns: code (A), name (B)
 */
function buildNameToEmpMap_(masterEmployeesFileId, sheetName, codeCol = 1, nameCol = 2) {
  const ss = SpreadsheetApp.openById(masterEmployeesFileId);
  const sh = ss.getSheetByName(sheetName);
  if (!sh) throw new Error('Không tìm thấy sheet ' + sheetName + ' trong file nhân sự');
  const lr = sh.getLastRow();
  const values = sh.getRange(1, 1, lr, Math.max(codeCol, nameCol)).getValues();
  const map = new Map(); // normalizedName -> code
  for (let r = 1; r < values.length; r++) {
    const code = String(values[r][codeCol - 1] || '').trim().toUpperCase();
    const name = String(values[r][nameCol - 1] || '').trim();
    if (!code || !name) continue;
    map.set(normalize_(name), code);
  }
  return map;
}

/**
 * Load schedule registrations (OFF entries) from provided schedule sheets
 * Returns Map<empCode, Map<dayStr, Set<sessionName>>> where sessionName in {'morning','afternoon','evening'}
 */
function loadScheduleRegistrations_(scheduleFileId, sheetNames, masterNameMap) {
  const ss = SpreadsheetApp.openById(scheduleFileId);
  const scheduleMap = new Map();
  const unmatched = new Set();

  // helper to map shift label to session
  const shiftToSession = (s) => {
    if (!s) return null;
    const t = String(s || '').toUpperCase();
    if (t.startsWith('ST')) return 'morning';
    if (t.startsWith('CT')) return 'afternoon';
    if (t.startsWith('TT') || t.startsWith('TTT') || t.startsWith('T')) return 'evening';
    // fallback: inspect words
    if (t.includes('SANG')) return 'morning';
    if (t.includes('CHIEU')) return 'afternoon';
    if (t.includes('TOI') || t.includes('TỐI')) return 'evening';
    return 'afternoon';
  };

  sheetNames.forEach(sheetName => {
    const sh = ss.getSheetByName(sheetName);
    if (!sh) { Logger.log('Warning: sheet not found ' + sheetName); return; }
    const lr = sh.getLastRow();
    const lc = sh.getLastColumn();
    const vals = sh.getRange(1, 1, lr, lc).getValues();

    // find header row with date cells and shift row (right under or second row)
    let dateRow = null, shiftRow = null, nameCol = null, nameHeaderRow = null;
    for (let r = 0; r < Math.min(10, vals.length); r++) {
      let dateCount = 0;
      for (let c = 0; c < Math.min(50, vals[r].length); c++) {
        const cell = vals[r][c];
        if (cell instanceof Date || /^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(String(cell || ''))) dateCount++;
      }
      if (dateCount >= 1 && dateRow === null) dateRow = r;
    }
    // shift row is likely the next row which contains labels like ST2, CT2, TT2
    if (dateRow !== null && dateRow + 1 < vals.length) shiftRow = dateRow + 1;

    // find employee-code column or name column by common headers
    let empCodeCol = null;
    for (let r = 0; r <= Math.min(10, vals.length - 1); r++) {
      for (let c = 0; c < Math.min(10, vals[r].length); c++) {
        const h = normalize_(vals[r][c] || '');
        if (!empCodeCol && (h.includes('ma') || h.includes('ma so') || h.includes('mã') || h.includes('mã số') || h.includes('ma nhan vien') || h.includes('mã nhân viên') || h.includes('mã nv') || h.includes('mã số nhân'))) {
          empCodeCol = c; nameHeaderRow = r; break;
        }
      }
      if (empCodeCol !== null) break;
    }

    if (empCodeCol === null) {
      // find name column as fallback
      for (let r = 0; r <= Math.min(10, vals.length - 1); r++) {
        for (let c = 0; c < Math.min(10, vals[r].length); c++) {
          const h = normalize_(vals[r][c] || '');
          if (h.includes('cvts') || h.includes('ho ten') || h.includes('họ tên') || h.includes('tên')) {
            nameCol = c; nameHeaderRow = r; break;
          }
        }
        if (nameCol !== null) break;
      }
    } else {
      // if emp code column found, we can still try to locate name column for logging/fallback
      for (let r = 0; r <= Math.min(10, vals.length - 1); r++) {
        for (let c = 0; c < Math.min(10, vals[r].length); c++) {
          const h = normalize_(vals[r][c] || '');
          if (h.includes('cvts') || h.includes('ho ten') || h.includes('họ tên') || h.includes('tên')) {
            nameCol = c; nameHeaderRow = r; break;
          }
        }
        if (nameCol !== null) break;
      }
    }

    if (empCodeCol === null && nameCol === null) {
      Logger.log('Could not find name/code column in ' + sheetName + ' - falling back to column B');
      nameCol = 1; nameHeaderRow = 0; // fallback
    }

    // determine date columns and their date string (day number)
    const dateCols = []; // array of {colIndex, dayStr, shiftLabel}
    if (dateRow !== null) {
      for (let c = 0; c < vals[dateRow].length; c++) {
        const v = vals[dateRow][c];
        let day = parseDayFromValue_(v);
        // Hỗ trợ format dd/mm/yyyy
        if (!day && /^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(String(v || '').trim())) {
          day = String(Number(String(v).split('/')[0]));
          if (!/^[1-9]$|^[12]\d$|^3[01]$/.test(day)) day = null;
        }
        if (day) {
          // shift label probably in shiftRow cell(s) under/near this column
          const shiftLabel = (shiftRow !== null && vals[shiftRow] && vals[shiftRow][c]) ? String(vals[shiftRow][c] || '') : '';
          dateCols.push({ col: c, dayStr: day, shiftLabel });
        }
      }
    }

    // iterate employee rows under nameHeaderRow or empCodeHeader
    const startRow = (nameHeaderRow !== null) ? nameHeaderRow + 1 : 2;
    for (let r = startRow; r < lr; r++) {
      // prefer empCode column if present
      let rawCode = (typeof empCodeCol === 'number') ? String(vals[r][empCodeCol] || '').trim().toUpperCase() : '';
      let empCode = '';

      if (rawCode && /^MH\d{4}$/i.test(rawCode)) {
        empCode = rawCode;
      } else {
        // fallback to name-based mapping
        const rawName = nameCol !== null ? String(vals[r][nameCol] || '').trim() : '';
        if (!rawName) continue;
        const n = normalize_(rawName.replace(/\(.*\)$/, '').trim()); // strip trailing (onl) etc
        empCode = masterNameMap.get(n) || '';
        if (!empCode) {
          // attempt fuzzy match: find any master name that contains all tokens
          const tokens = n.split(' ');
          for (const [mn, code] of masterNameMap.entries()) {
            let ok = true;
            for (const t of tokens) if (t && !mn.includes(t)) { ok = false; break; }
            if (ok) { empCode = code; break; }
          }
        }
        if (!empCode && rawCode) {
          // rawCode present but not standard format; try to extract MHxxxx inside rawCode
          const m = String(rawCode).match(/(MH\d{4})/i);
          if (m) empCode = m[1].toUpperCase();
        }
        if (!empCode) { unmatched.add(rawName || rawCode); continue; }
      }

      // for each date col, check if the cell value is 'OFF' or equivalent
      // Chỉ thêm vào scheduleMap nếu có ít nhất 1 đăng ký OFF
      let hasOffRegistration = false;
      const empSched = new Map();

      for (const dc of dateCols) {
        const c = dc.col;
        const cellVal = String(vals[r][c] || '').trim().toUpperCase();
        if (cellVal === 'OFF' || cellVal === 'OFF ▼' || cellVal.startsWith('OFF')) {
          hasOffRegistration = true;
          const sess = shiftToSession(dc.shiftLabel || '');
          if (!empSched.has(dc.dayStr)) empSched.set(dc.dayStr, new Set());
          empSched.get(dc.dayStr).add(sess);
        }
      }

      // Chỉ thêm vào scheduleMap nếu có ít nhất 1 đăng ký OFF
      if (hasOffRegistration) {
        scheduleMap.set(empCode, empSched);
      }
    }
  });

  if (unmatched.size) Logger.log('Schedule load: unmatched names count=' + unmatched.size + ' sample=' + Array.from(unmatched).slice(0, 5).join(', '));
  return scheduleMap;
}

/**
 * Load ALL schedule registrations (not just OFF) from provided schedule sheets
 * Returns Map<empCode, Map<dayStr, Set<sessionName>>> where sessionName in {'morning','afternoon','evening'}
 * Khác với loadScheduleRegistrations_ chỉ load OFF, hàm này load tất cả các ca đăng ký (có giá trị, không phải OFF, không phải trống)
 */
function loadAllScheduleRegistrations_(scheduleFileId, sheetNames, masterNameMap) {
  const ss = SpreadsheetApp.openById(scheduleFileId);
  const scheduleMap = new Map();
  const unmatched = new Set();

  // helper to map shift label to session
  const shiftToSession = (s) => {
    if (!s) return null;
    const t = String(s || '').toUpperCase();
    if (t.startsWith('ST')) return 'morning';
    if (t.startsWith('CT')) return 'afternoon';
    if (t.startsWith('TT') || t.startsWith('TTT') || t.startsWith('T')) return 'evening';
    // fallback: inspect words
    if (t.includes('SANG')) return 'morning';
    if (t.includes('CHIEU')) return 'afternoon';
    if (t.includes('TOI') || t.includes('TỐI')) return 'evening';
    return 'afternoon';
  };

  sheetNames.forEach(sheetName => {
    const sh = ss.getSheetByName(sheetName);
    if (!sh) { Logger.log('Warning: sheet not found ' + sheetName); return; }
    const lr = sh.getLastRow();
    const lc = sh.getLastColumn();
    const vals = sh.getRange(1, 1, lr, lc).getValues();

    // find header row with date cells and shift row (right under or second row)
    let dateRow = null, shiftRow = null, nameCol = null, nameHeaderRow = null;
    for (let r = 0; r < Math.min(10, vals.length); r++) {
      let dateCount = 0;
      for (let c = 0; c < Math.min(50, vals[r].length); c++) {
        const cell = vals[r][c];
        if (cell instanceof Date || /^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(String(cell || ''))) dateCount++;
      }
      if (dateCount >= 1 && dateRow === null) dateRow = r;
    }
    // shift row is likely the next row which contains labels like ST2, CT2, TT2
    if (dateRow !== null && dateRow + 1 < vals.length) shiftRow = dateRow + 1;

    // find employee-code column or name column by common headers
    let empCodeCol = null;
    for (let r = 0; r <= Math.min(10, vals.length - 1); r++) {
      for (let c = 0; c < Math.min(10, vals[r].length); c++) {
        const h = normalize_(vals[r][c] || '');
        if (!empCodeCol && (h.includes('ma') || h.includes('ma so') || h.includes('mã') || h.includes('mã số') || h.includes('ma nhan vien') || h.includes('mã nhân viên') || h.includes('mã nv') || h.includes('mã số nhân'))) {
          empCodeCol = c; nameHeaderRow = r; break;
        }
      }
      if (empCodeCol !== null) break;
    }

    if (empCodeCol === null) {
      // find name column as fallback
      for (let r = 0; r <= Math.min(10, vals.length - 1); r++) {
        for (let c = 0; c < Math.min(10, vals[r].length); c++) {
          const h = normalize_(vals[r][c] || '');
          if (h.includes('cvts') || h.includes('ho ten') || h.includes('họ tên') || h.includes('tên')) {
            nameCol = c; nameHeaderRow = r; break;
          }
        }
        if (nameCol !== null) break;
      }
    } else {
      // if emp code column found, we can still try to locate name column for logging/fallback
      for (let r = 0; r <= Math.min(10, vals.length - 1); r++) {
        for (let c = 0; c < Math.min(10, vals[r].length); c++) {
          const h = normalize_(vals[r][c] || '');
          if (h.includes('cvts') || h.includes('ho ten') || h.includes('họ tên') || h.includes('tên')) {
            nameCol = c; nameHeaderRow = r; break;
          }
        }
        if (nameCol !== null) break;
      }
    }

    if (empCodeCol === null && nameCol === null) {
      Logger.log('Could not find name/code column in ' + sheetName + ' - falling back to column B');
      nameCol = 1; nameHeaderRow = 0; // fallback
    }

    // determine date columns and their date string (day number)
    const dateCols = []; // array of {colIndex, dayStr, shiftLabel}
    if (dateRow !== null) {
      for (let c = 0; c < vals[dateRow].length; c++) {
        const v = vals[dateRow][c];
        let day = parseDayFromValue_(v);
        // Hỗ trợ format dd/mm/yyyy
        if (!day && /^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(String(v || '').trim())) {
          day = String(Number(String(v).split('/')[0]));
          if (!/^[1-9]$|^[12]\d$|^3[01]$/.test(day)) day = null;
        }
        if (day) {
          // shift label probably in shiftRow cell(s) under/near this column
          const shiftLabel = (shiftRow !== null && vals[shiftRow] && vals[shiftRow][c]) ? String(vals[shiftRow][c] || '') : '';
          dateCols.push({ col: c, dayStr: day, shiftLabel });
        }
      }
    }

    // iterate employee rows under nameHeaderRow or empCodeHeader
    const startRow = (nameHeaderRow !== null) ? nameHeaderRow + 1 : 2;
    for (let r = startRow; r < lr; r++) {
      // prefer empCode column if present
      let rawCode = (typeof empCodeCol === 'number') ? String(vals[r][empCodeCol] || '').trim().toUpperCase() : '';
      let empCode = '';

      if (rawCode && /^MH\d{4}$/i.test(rawCode)) {
        empCode = rawCode;
      } else {
        // fallback to name-based mapping
        const rawName = nameCol !== null ? String(vals[r][nameCol] || '').trim() : '';
        if (!rawName) continue;
        const n = normalize_(rawName.replace(/\(.*\)$/, '').trim()); // strip trailing (onl) etc
        empCode = masterNameMap.get(n) || '';
        if (!empCode) {
          // attempt fuzzy match: find any master name that contains all tokens
          const tokens = n.split(' ');
          for (const [mn, code] of masterNameMap.entries()) {
            let ok = true;
            for (const t of tokens) if (t && !mn.includes(t)) { ok = false; break; }
            if (ok) { empCode = code; break; }
          }
        }
        if (!empCode && rawCode) {
          // rawCode present but not standard format; try to extract MHxxxx inside rawCode
          const m = String(rawCode).match(/(MH\d{4})/i);
          if (m) empCode = m[1].toUpperCase();
        }
        if (!empCode) { unmatched.add(rawName || rawCode); continue; }
      }

      // ensure map has entry
      if (!scheduleMap.has(empCode)) scheduleMap.set(empCode, new Map());
      const empSched = scheduleMap.get(empCode);

      // for each date col, check if the cell has ANY value (not empty, not OFF) = đăng ký ca
      for (const dc of dateCols) {
        const c = dc.col;
        const cellVal = String(vals[r][c] || '').trim().toUpperCase();
        // Nếu có giá trị và không phải OFF = đăng ký ca
        if (cellVal && cellVal !== 'OFF' && !cellVal.startsWith('OFF')) {
          const sess = shiftToSession(dc.shiftLabel || '');
          if (sess) {
            if (!empSched.has(dc.dayStr)) empSched.set(dc.dayStr, new Set());
            empSched.get(dc.dayStr).add(sess);
          }
        }
      }
    }
  });

  if (unmatched.size) Logger.log('All schedule load: unmatched names count=' + unmatched.size + ' sample=' + Array.from(unmatched).slice(0, 5).join(', '));
  return scheduleMap;
}

/**
 * Prepare attendance changes by using schedule registrations (extracted OFFs) and raw times
 * This function will mark missing check-ins for scheduled sessions and produce notes per row
 */
function prepareAttendanceChangesWithSchedule_(timesByEmpDay, masterInfo, scheduleMap, cfg, month) {
  const changes = new Map();
  const problematicCells = [];

  // helper to convert session name to human
  const humanForSession = (sess) => {
    if (!sess) return 'ca';
    if (sess === 'morning') return 'ca sáng';
    if (sess === 'afternoon') return 'ca chiều';
    if (sess === 'evening') return 'ca tối';
    return 'ca';
  };

  // Only check employees who have schedule registrations (not empty days)
  // If a day is empty (not in scheduleMap), skip checking - it's considered valid
  // iterate each scheduled employee (only those with registered schedules)
  for (const [emp, dayMap] of scheduleMap.entries()) {
    const r1 = masterInfo.empToRow.get(emp);
    if (!r1) continue; // employee not in master attendance sheet
    const r0 = r1 - 1;

    let notesForDetail = [];
    let lateCount = 0, offForgotCount = 0;

    const role = masterInfo.empToRole ? masterInfo.empToRole.get(emp) : undefined;
    const scheduleTemplate = getEmployeeSchedule_(emp, cfg, role);

    for (const [dayStr, sessSet] of dayMap.entries()) {
      // get times from raw map first
      const rawDayMap = timesByEmpDay.get(emp);
      const timesArr = rawDayMap && rawDayMap.get(dayStr) ? Array.from(rawDayMap.get(dayStr)) : [];

      // if no raw times, check masterInfo dayBlock cell (maybe times already merged there)
      if ((!timesArr || timesArr.length === 0) && masterInfo) {
        const col1 = masterInfo.colByDay.get(dayStr);
        if (col1) {
          const c0 = col1 - masterInfo.minDayCol;
          const existing = masterInfo.dayBlock[r0][c0];
          const extracted = extractTimesFromCell_(existing);
          if (extracted && extracted.length) {
            timesArr.push(...extracted);
          }
        }
      }

      if (!timesArr || timesArr.length === 0) {
        // no times: missing check-ins for all scheduled sessions
        // Tất cả lỗi đều đếm vào offForgotCount (cột U)
        for (const sess of Array.from(sessSet)) {
          const human = humanForSession(sess);
          notesForDetail.push(`- Quên check in ${human} ${dayStr}/${month}`);
          offForgotCount++;
        }
      } else {
        // Check if too many check-ins/outs in a single cell
        const threshold = cfg.maxTimesThreshold || 4;
        if (timesArr && timesArr.length > threshold) {
          const col1 = masterInfo.colByDay.get(dayStr);
          if (col1) {
            const c0 = col1 - masterInfo.minDayCol;
            if (c0 >= 0 && c0 < masterInfo.dayColsCount) {
              problematicCells.push({
                r0,
                c0,
                emp,
                dayStr,
                type: 'tooManyTimes',
                timesCount: timesArr.length
              });
              Logger.log(`SKIP cell r=${r0 + 1} c=${c0 + masterInfo.minDayCol} emp=${emp} day=${dayStr} times=${timesArr.length} (> ${threshold})`);
            }
          }
          continue; // Skip processing this cell
        }

        // there are times -> compute sessions and check against registered sessions
        const sessionsMap = computeSessionsBySchedule_(timesArr, scheduleTemplate, sessSet);

        // Kiểm tra check-in trễ và thêm vào problematicCells
        const col1 = masterInfo.colByDay.get(dayStr);
        if (col1) {
          const c0 = col1 - masterInfo.minDayCol;
          if (c0 >= 0 && c0 < masterInfo.dayColsCount) {
            // Kiểm tra tất cả sessions để phát hiện check-in trễ (không chỉ các ca đã đăng ký)
            for (const [sessionName, session] of Object.entries(sessionsMap)) {
              // Bỏ qua các session không hợp lệ
              if (sessionName === '_problematic' || sessionName === '_timesCount') continue;

              // Kiểm tra nếu có check-in và có lateMinutes > 0
              // lateMinutes đã được tính dựa trên schedule của từng role
              if (session.in && session.lateMinutes && session.lateMinutes > 0) {
                // Có check-in trễ
                problematicCells.push({
                  r0,
                  c0,
                  emp,
                  dayStr,
                  type: 'late',
                  sessionName: sessionName,
                  lateMinutes: session.lateMinutes,
                  checkInTime: session.in
                });
                Logger.log(`LATE CHECK-IN cell r=${r0 + 1} c=${c0 + masterInfo.minDayCol} emp=${emp} day=${dayStr} session=${sessionName} late=${session.lateMinutes}min checkIn=${session.in}`);
              }
            }
          }
        }

        // CHỈ kiểm tra các ca đã đăng ký trong schedule (sessSet), không kiểm tra tất cả sessions
        for (const registeredSess of Array.from(sessSet)) {
          // Map registered session (morning/afternoon/evening) to session name in sessionsMap
          let sessionFound = null;
          for (const [sessionName, session] of Object.entries(sessionsMap)) {
            const sessionNameLower = sessionName.toLowerCase();
            const registeredLower = String(registeredSess).toLowerCase();

            if (sessionNameLower === registeredLower ||
              (registeredLower === 'morning' && (sessionNameLower.includes('sang') || sessionNameLower === 'morning')) ||
              (registeredLower === 'afternoon' && (sessionNameLower.includes('chieu') || sessionNameLower === 'afternoon')) ||
              (registeredLower === 'evening' && (sessionNameLower.includes('toi') || sessionNameLower === 'evening'))) {
              sessionFound = session;
              break;
            }
          }

          const human = humanForSession(registeredSess);

          // Nếu không tìm thấy session trong chấm công = quên check-in
          if (!sessionFound || sessionFound.in == null) {
            notesForDetail.push(`- Quên check in ${human} ${dayStr}/${month}`);
            offForgotCount++;
          } else if (sessionFound.out == null) {
            // Có check-in nhưng quên check-out
            notesForDetail.push(`- Quên check out ${human} ${dayStr}/${month}`);
            offForgotCount++;
          }

        }
      }
    }

    if (notesForDetail.length) changes.set(r0, { notes: notesForDetail, lateCount, offForgotCount });
  }

  return { changes, problematicCells };
}

/**
 * Xử lý chấm công đơn giản - chỉ dựa vào raw data, không cần đăng ký ca
 * Phân tích trực tiếp từ times để phát hiện lỗi: TRỄ, QUÊN IN/OUT.
 * Có thể chạy theo 3 mode:
 *  - 'both'   : xử lý cả TRỄ và QUÊN IN/OUT (mặc định, giống logic cũ)
 *  - 'late'   : chỉ xử lý TRỄ (bỏ qua hoàn toàn lỗi quên check in/out)
 *  - 'missing': chỉ xử lý QUÊN check in/out (bỏ qua hoàn toàn lỗi trễ)
 * @param {Map} timesByEmpDay - Map<empCode, Map<dayStr, Set<times>>>
 * @param {Object} masterInfo - Thông tin master sheet
 * @param {Object} cfg - Config (morningStart, afternoonStart, cutoff, lateThreshold)
 * @param {number} month - Tháng
 * @param {string} mode - 'both' | 'late' | 'missing'
 * @return {Object} { changes: Map<r0, {notes, lateCount, offForgotCount}>, problematicCells: Array }
 */
function prepareAttendanceChangesSimple_(timesByEmpDay, masterInfo, cfg, month, mode) {
  const changes = new Map();
  const problematicCells = [];
  const normalizedMode = (String(mode || 'both').toLowerCase());
  const runLate = (normalizedMode === 'both' || normalizedMode === 'late');
  const runMissing = (normalizedMode === 'both' || normalizedMode === 'missing');

  // helper to convert session name to human
  const humanForSession = (sess) => {
    if (!sess) return 'ca';
    if (sess === 'morning') return 'ca sáng';
    if (sess === 'afternoon') return 'ca chiều';
    if (sess === 'evening') return 'ca tối';
    return 'ca';
  };

  // Duyệt đúng theo master sheet (cùng cách với highlight) để đọc cùng ô, tránh sai sót do key/casing từ raw.
  for (const [emp, r1] of masterInfo.empToRow.entries()) {
    // YÊU CẦU: Bỏ qua hoàn toàn nhân viên MH0008 (không xử lý trễ, không quên in/out)
    const empId = String(emp || '').toUpperCase();
    if (empId === 'MH0008') continue;

    const r0 = r1 - 1;

    let notesForDetail = [];
    let lateCount = 0, offForgotCount = 0;

    // Lấy schedule template dựa trên role (chỉ để tính lateMinutes, không dùng để check đăng ký ca)
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(emp) : undefined;
    const scheduleTemplate = getEmployeeSchedule_(emp, cfg, role);

    // Duyệt qua TẤT CẢ các ngày từ 1 đến 31
    for (let dayNum = 1; dayNum <= 31; dayNum++) {
      const dayStr = String(dayNum);
      let timesArr = [];
      let masterHasAnyValue = false;

      // ƯU TIÊN: Đọc từ master sheet trước (dữ liệu đã chỉnh sửa thủ công)
      if (masterInfo) {
        const col1 = masterInfo.colByDay.get(dayStr);
        if (col1) {
          const c0 = col1 - masterInfo.minDayCol;
          if (c0 >= 0 && c0 < masterInfo.dayColsCount) {
            const existing = masterInfo.dayBlock[r0][c0];
            masterHasAnyValue = (existing !== null && existing !== undefined && String(existing).trim() !== '');
            const extracted = extractTimesFromCell_(existing);
            if (extracted && extracted.length) {
              timesArr.push(...extracted);
            }
          }
        }
      }

      // FALLBACK: Nếu master sheet không có dữ liệu, mới lấy từ raw data
      // LƯU Ý: Nếu ô master có dữ liệu nhưng parse không ra HH:mm (do chỉnh sửa format),
      // thì KHÔNG fallback về raw để tránh xử lý theo data nhập về (bỏ qua thay đổi thủ công).
      if ((!timesArr || timesArr.length === 0) && !masterHasAnyValue && timesByEmpDay.has(emp)) {
        const rawDayMap = timesByEmpDay.get(emp);
        if (rawDayMap && rawDayMap.has(dayStr)) {
          const timesSet = rawDayMap.get(dayStr);
          timesArr = Array.from(timesSet);
        }
      }

      // Bỏ qua nếu không có dữ liệu (không tính vắng, không check schedule)
      if (!timesArr || timesArr.length === 0) {
        continue;
      }

      // Check if too many check-ins/outs in a single cell
      const threshold = cfg.maxTimesThreshold || 4;
      if (timesArr && timesArr.length > threshold) {
        const col1 = masterInfo.colByDay.get(dayStr);
        if (col1) {
          const c0 = col1 - masterInfo.minDayCol;
          if (c0 >= 0 && c0 < masterInfo.dayColsCount) {
            problematicCells.push({
              r0,
              c0,
              emp,
              dayStr,
              type: 'tooManyTimes',
              timesCount: timesArr.length
            });
            Logger.log(`SKIP cell r=${r0 + 1} c=${c0 + masterInfo.minDayCol} emp=${emp} day=${dayStr} times=${timesArr.length} (> ${threshold})`);
          }
        }
        // Không bỏ qua cell này nữa, vẫn tiếp tục phân tích để không sót lỗi
      }

      // Phân tích times để xác định check-in/out (KHÔNG cần registeredSessions)
      // Truyền null cho registeredSessions để hàm tự phân tích dựa trên thời gian
      const sessionsMap = computeSessionsBySchedule_(timesArr, scheduleTemplate, null);

      // Kiểm tra tất cả sessions được phát hiện (morning, afternoon)
      for (const [sessionName, session] of Object.entries(sessionsMap)) {
        // Bỏ qua các session không hợp lệ
        if (sessionName === '_problematic' || sessionName === '_timesCount') continue;

        // Với chế độ đơn giản: bỏ qua session không có dữ liệu. Nhưng vẫn xử lý session có in/out/lateMinutes
        // (phòng edge case session.times rỗng nhưng in/out đã được set, ví dụ quản lý 4 mốc)
        const hasTimes = session && Array.isArray(session.times) && session.times.length > 0;
        const hasInOutOrLate = session && (session.in != null || session.out != null || (typeof session.lateMinutes === 'number' && session.lateMinutes > 0));
        if (!session || (!hasTimes && !hasInOutOrLate)) {
          continue;
        }

        const human = humanForSession(sessionName);
        const totalTimesInDay = Array.isArray(timesArr) ? timesArr.length : 0;

        // 1) Xử lý QUÊN CHECK IN/OUT (tùy theo mode)
        if (runMissing) {
          const missingRes = handleMissingCheckInOutSimple_(session, human, dayStr, month, scheduleTemplate, sessionName, totalTimesInDay);
          if (missingRes.notes.length) {
            notesForDetail.push(...missingRes.notes);
            offForgotCount += missingRes.offForgotDelta;
          }
        }

        // 2) Xử lý TRỄ CHECK-IN (tùy theo mode)
        if (runLate) {
          const lateRes = handleLateSimple_(session, human, dayStr, month, masterInfo, r0, emp, sessionName, problematicCells);
          if (lateRes.notes.length) {
            notesForDetail.push(...lateRes.notes);
            lateCount += lateRes.lateDelta;
            offForgotCount += lateRes.offForgotDelta;
          }
        }
      }
    }

    if (notesForDetail.length) {
      changes.set(r0, { notes: notesForDetail, lateCount, offForgotCount });
    }
  }

  return { changes, problematicCells };
}

// Các hàm schedule-aware đã được xóa vì không còn cần thiết
// Sử dụng applyAttendance() với useSchedule=false (mặc định) để xử lý chỉ dựa vào raw data

/**
 * Unified apply function for attendance checks - chỉ dựa vào raw data, không cần schedule
 * Options:
 *  - dryRun: boolean (true = preview only)
 *  - testRows: number|null (if set >0, will write only N changed rows for safe testing)
 *  - mode: 'both' | 'late' | 'missing'
 */
function applyAttendance(opts) {
  opts = opts || {};
  // Mặc định: chạy commit (dryRun=false) để ghi thẳng vào sheet nếu không truyền opts.dryRun
  const dryRun = typeof opts.dryRun === 'boolean' ? opts.dryRun : false;
  const testRows = (typeof opts.testRows === 'number' && opts.testRows > 0) ? Math.trunc(opts.testRows) : null;

  const MASTER_FILE_ID = '1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4';
  const MASTER_SHEET_NAME = 'Chấm công th12/2025';

  const RAW_FILE_ID = '1ed1IK4X1bQxhBoz4tjUKEypIv6cipNKsUCcXPKjqy8o';
  const RAW_SHEETS = ['L4_HH'];
  const cfg = { morningStart: '08:30', afternoonStart: '13:15', cutoff: '12:00', lateThreshold: 30, maxTimesThreshold: 4 };

  // Load raw data
  Logger.log('1) Loading raw data from sheets...');
  const timesByEmpDay = buildTimesFromRawSheets_(RAW_FILE_ID, RAW_SHEETS);
  Logger.log('   Loaded times for ' + timesByEmpDay.size + ' employees');

  // Load master sheet info - dùng cùng khối cột vân tay AJ-BN (36-66) như highlight để đọc đúng ô và ghi note đúng hàng
  const FINGERPRINT_DAY_COL_MIN = 36;  // AJ
  const FINGERPRINT_DAY_COL_MAX = 66; // BN
  Logger.log('2) Loading master sheet info...');
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, 2, 1, FINGERPRINT_DAY_COL_MIN, FINGERPRINT_DAY_COL_MAX);
  const month = parseMonthFromSheetName_(MASTER_SHEET_NAME) || 12;
  const headerMap = findHeaderCols_(masterInfo.header);

  // Compute changes - chỉ dựa vào raw data, không cần schedule
  Logger.log('3) Analyzing attendance (simple mode - no schedule check)...');
  const mode = opts.mode || 'both'; // 'both' | 'late' | 'missing'
  const result = prepareAttendanceChangesSimple_(timesByEmpDay, masterInfo, cfg, month, mode);

  let changes = result.changes || new Map();
  const problematicCells = result.problematicCells || [];

  // Đồng bộ note trễ từ problematicCells vào changes để không sót (ô tô đỏ nhưng không có note, ví dụ MH0029)
  const runLateSync = (mode === 'both' || mode === 'late');
  if (runLateSync && problematicCells.length > 0) {
    const humanMap = { morning: 'ca sáng', afternoon: 'ca chiều', evening: 'ca tối' };
    for (const p of problematicCells) {
      if (p.type !== 'late' && p.type !== 'missing_in_over_30') continue;
      const r0 = p.r0;
      const human = humanMap[p.sessionName] || 'ca';
      const lateMin = Math.round(Number(p.lateMinutes) || 0);
      const threshold = 30;
      const noteStr = lateMin > threshold
        ? `- Check in trễ từ 30 phút trở lên (${lateMin} phút) ${human} ${p.dayStr}/${month}`
        : `- Check in trễ dưới 30 phút (${lateMin} phút) ${human} ${p.dayStr}/${month}`;
      if (!changes.has(r0)) changes.set(r0, { notes: [], lateCount: 0, offForgotCount: 0 });
      const entry = changes.get(r0);
      const alreadyHas = entry.notes.some(n => typeof n === 'string' && n.includes(p.dayStr) && n.includes(human));
      if (!alreadyHas) {
        entry.notes.push(noteStr);
        if (lateMin > threshold) entry.offForgotCount = (entry.offForgotCount || 0) + 1; else entry.lateCount = (entry.lateCount || 0) + 1;
      }
    }
  }

  Logger.log('   Computed changes=' + changes.size + ' problematic=' + problematicCells.length);

  // prepare arrays for writing
  const lastEmpRow = masterInfo.lastEmpRow;
  // Ghi note:
  //  - lateNoteCol (thường là cột S / Chi tiết(2)) cho các note TRỄ
  //  - noteCol (cột W / Chi tiết(3)) cho các note QUÊN CHECK IN/OUT
  const noteCol = headerMap.noteCol || headerMap.detail3Col || headerMap.detail2Col;
  const lateNoteCol = headerMap.detail2Col || null;
  const noteArr = noteCol ? masterSh.getRange(1, noteCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];
  const lateNoteArr = lateNoteCol ? masterSh.getRange(1, lateNoteCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];
  const totalLateArr = headerMap.totalLateCol ? masterSh.getRange(1, headerMap.totalLateCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];
  const offForgotArr = headerMap.offForgotCol ? masterSh.getRange(1, headerMap.offForgotCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];

  const newNote = noteArr.slice();
  const newLateNote = lateNoteArr.slice();
  const newTotalLate = totalLateArr.slice();
  const newOffForgot = offForgotArr.slice();

  // apply changes into arrays
  for (const [r0, v] of changes.entries()) {
    const allNotes = v.notes || [];
    // Cột S: chỉ ghi note TRỄ dưới/đúng 30 phút; loại note "trễ từ 30 phút trở lên" (ghi vào W)
    const lateNotes = allNotes.filter(n => {
      if (typeof n !== 'string') return false;
      const nl = (n || '').toLowerCase();
      return nl.includes('trễ') && !nl.includes('trễ từ 30 phút trở lên');
    });
    // Missing notes (cột W): gồm các lỗi "quên check" và các case trễ bị quy thành lỗi trên cột W (>=30p theo wording)
    const missingNotes = allNotes.filter(n => {
      if (typeof n !== 'string') return false;
      const nl = n.toLowerCase();
      return nl.includes('quên check') || nl.includes('check in trễ từ 30 phút trở lên');
    });

    // Cột S (trễ): ghi đè bằng đúng note TRỄ của lần chạy này. Case trễ > 30 đã chuyển quên check thì không nằm trong lateNotes → không ghi vào S (chỉ ghi vào W).
    // Trễ <= 30 phút (bao gồm cả = 30 phút) là lỗi TRỄ, ghi vào cột S.
    if (lateNoteCol) {
      newLateNote[r0] = lateNotes.length ? lateNotes.join('\n') : '';
    }
    // Cột W (quên check): append note QUÊN CHECK (gồm cả case chuyển từ "trễ > 30" thành quên check in)
    if (noteCol && missingNotes.length) {
      const prev = String(newNote[r0] || '').trim();
      newNote[r0] = (prev ? prev + '\n' : '') + missingNotes.join('\n');
    }
    // Cột Q (TỔNG TRỄ OFF): đếm đúng số lỗi trễ chấm công vân tay = số dòng note trong cột S
    if (headerMap.totalLateCol) {
      newTotalLate[r0] = lateNotes.length;
    }
    // Cột U (OFF QUÊN CHECK IN/OUT/TRỄ >=30p): đếm đúng số lỗi = số dòng note trong cột W (sau khi đã cập nhật)
    if (headerMap.offForgotCol) {
      const wContent = String(newNote[r0] || '').trim();
      newOffForgot[r0] = wContent ? wContent.split(/\n/).filter(line => line.trim()).length : 0;
    }
    // Bỏ xử lý onlForgotCol - không sử dụng nữa
  }

  // preview
  if (dryRun) {
    Logger.log('applyAttendance (dryRun) preview sample:');
    let i = 0;
    for (const [r0, v] of changes.entries()) {
      Logger.log(`PREVIEW Row ${r0 + 1}: ${v.notes.join('; ')}`);
      if (++i >= 50) break;
    }
    return { changesCount: changes.size, problematicCellsCount: problematicCells.length };
  }

  // if testRows specified -> write per-row for first N changes (safe)
  const changeEntries = Array.from(changes.entries());
  if (testRows) {
    Logger.log('applyAttendance: testRows write mode N=' + testRows + ' (per-row writes)');
    const slice = changeEntries.slice(0, testRows);
    slice.forEach(([r0, v]) => {
      const rowNum = r0 + 1;
      const lateNotes = (v.notes || []).filter(n => {
        if (typeof n !== 'string') return false;
        const nl = (n || '').toLowerCase();
        return nl.includes('trễ') && !nl.includes('trễ từ 30 phút trở lên');
      });
      const missingNotes = (v.notes || []).filter(n => {
        if (typeof n !== 'string') return false;
        const nl = n.toLowerCase();
        return nl.includes('quên check') || nl.includes('check in trễ từ 30 phút trở lên');
      });
      if (headerMap.detail2Col) {
        masterSh.getRange(rowNum, headerMap.detail2Col).setValue(lateNotes.length ? lateNotes.join('\n') : '');
      }
      const noteColToWrite = headerMap.noteCol || headerMap.detail3Col || headerMap.detail2Col;
      let newWContent = String(noteArr[r0] || '').trim();
      if (noteColToWrite && missingNotes.length) {
        newWContent = (newWContent ? newWContent + '\n' : '') + missingNotes.join('\n');
        masterSh.getRange(rowNum, noteColToWrite).setValue(newWContent);
      }
      // Cột Q (TỔNG TRỄ OFF): đếm đúng số lỗi trễ = số dòng trong cột S
      if (headerMap.totalLateCol) masterSh.getRange(rowNum, headerMap.totalLateCol).setValue(lateNotes.length);
      // Cột U (OFF QUÊN CHECK IN/OUT/TRỄ >=30p): đếm đúng số lỗi = số dòng trong cột W
      if (headerMap.offForgotCol) {
        const uCount = newWContent ? newWContent.split(/\n/).filter(line => line.trim()).length : 0;
        masterSh.getRange(rowNum, headerMap.offForgotCol).setValue(uCount);
      }
      Logger.log(`WROTE row ${rowNum}`);
    });
    return { changesCount: changeEntries.length, written: slice.length };
  }

  // otherwise full commit: write columns back
  const writes = [];
  const noteColToWrite = headerMap.noteCol || headerMap.detail3Col || headerMap.detail2Col;
  const lateNoteColToWrite = headerMap.detail2Col || null;
  if (noteColToWrite) writes.push({ range: masterSh.getRange(1, noteColToWrite, lastEmpRow, 1), values: newNote.map(x => [x || '']) });
  if (lateNoteColToWrite) writes.push({ range: masterSh.getRange(1, lateNoteColToWrite, lastEmpRow, 1), values: newLateNote.map(x => [x || '']) });
  if (headerMap.totalLateCol) writes.push({ range: masterSh.getRange(1, headerMap.totalLateCol, lastEmpRow, 1), values: newTotalLate.map(x => [x || 0]) });
  if (headerMap.offForgotCol) writes.push({ range: masterSh.getRange(1, headerMap.offForgotCol, lastEmpRow, 1), values: newOffForgot.map(x => [x || 0]) });
  // Bỏ xử lý onlForgotCol - không sử dụng nữa

  writes.forEach(w => w.range.setValues(w.values));
  Logger.log('applyAttendance: full commit wrote ' + writes.length + ' ranges');
  return { changesCount: changes.size, written: writes.length };
}

/**
 * Wrapper: chỉ chạy phần TRỄ (late) – không ghi/quét lỗi quên check in/out.
 * Có thể gán hàm này vào 1 nút/menu riêng.
 */
function applyAttendanceLateOnly(opts) {
  opts = opts || {};
  opts.mode = 'late';
  return applyAttendance(opts);
}

/**
 * Wrapper: chỉ chạy phần QUÊN CHECK IN/OUT – không xử lý TRỄ.
 * Có thể gán hàm này vào 1 nút/menu riêng.
 */
function applyAttendanceMissingOnly(opts) {
  opts = opts || {};
  opts.mode = 'missing';
  return applyAttendance(opts);
}

// Các hàm schedule-aware đã được xóa vì không còn cần thiết

/**
 * Cập nhật cột BU (TỔNG ca off vân tay) = số ca làm trong tháng, đếm từ dữ liệu vân tay (cột ngày AJ-BN).
 * Mỗi ca có đủ check-in và check-out được tính 1 ca.
 * @param {boolean} dryRun - Nếu true chỉ preview, không ghi vào sheet
 * @return {Object} - { employeesCount, updatedCount, dryRun?, buCol? }
 */
function updateTongCaOffVanTay(dryRun = true) {
  const MASTER_FILE_ID = "1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4";
  const MASTER_SHEET_NAME = "Chấm công th12/2025";
  const FINGERPRINT_DAY_COL_MIN = 36;
  const FINGERPRINT_DAY_COL_MAX = 66;
  const cfg = { morningStart: "08:30", afternoonStart: "13:15", cutoff: "12:00", lateThreshold: 30, maxTimesThreshold: 4 };

  Logger.log("1) Opening master sheet...");
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) throw new Error("Không tìm thấy sheet tổng: " + MASTER_SHEET_NAME);

  const masterInfo = buildMasterInfo_(masterSh, 2, 1, FINGERPRINT_DAY_COL_MIN, FINGERPRINT_DAY_COL_MAX);
  const headerMap = findHeaderCols_(masterInfo.header);
  const buCol = headerMap.totalCaOffVanTayCol;
  if (!buCol) {
    throw new Error("Không tìm thấy cột TỔNG ca off vân tay (BU) trong sheet tổng");
  }

  const lastEmpRow = masterInfo.lastEmpRow;
  const totals = []; // totals[r0] = số ca làm trong tháng

  Logger.log("2) Calculating total sessions per employee (from fingerprint data)...");
  for (const [emp, r1] of masterInfo.empToRow.entries()) {
    const r0 = r1 - 1;
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(emp) : undefined;
    const schedule = getEmployeeSchedule_(emp, cfg, role);
    const count = countCompleteSessionsInMonth_(r0, masterInfo, schedule, cfg);
    totals[r0] = count;
  }

  if (dryRun) {
    Logger.log("PREVIEW (dryRun) - Sample TỔNG ca off vân tay (BU):");
    let i = 0;
    for (const [emp, r1] of masterInfo.empToRow.entries()) {
      const r0 = r1 - 1;
      Logger.log(`  Row ${r1} (${emp}): ${totals[r0] || 0} ca`);
      if (++i >= 15) break;
    }
    return { employeesCount: masterInfo.empToRow.size, dryRun: true, buCol };
  }

  Logger.log("3) Writing column BU (TỔNG ca off vân tay)...");
  const headerValue = masterSh.getRange(1, buCol, 1, 1).getValue();
  const writeValues = [[headerValue]];
  for (let r0 = 1; r0 < lastEmpRow; r0++) {
    writeValues.push([totals[r0] != null ? totals[r0] : 0]);
  }
  const range = masterSh.getRange(1, buCol, lastEmpRow, 1);
  range.setValues(writeValues);

  Logger.log("4) Done. Updated TỔNG ca off vân tay (BU) for " + (lastEmpRow - 1) + " rows.");
  return { employeesCount: lastEmpRow - 1, updatedCount: lastEmpRow - 1, dryRun: false, buCol };
}

/**
 * Ghi trực tiếp cột BU (không preview).
 */
function updateTongCaOffVanTayCommit() {
  return updateTongCaOffVanTay(false);
}

// ==================== XỬ LÝ CHECK IN/OUT ONL TỪ GOOGLE FORM ====================

/**
 * Load dữ liệu check in/out ONL từ Google Form responses
 * @param {string} formFileId - File ID của Google Form responses sheet
 * @return {Array} Array of objects với format:
 *   {
 *     timestamp: Date,      // Thời gian check-in/out
 *     fullName: string,     // Họ tên
 *     checkType: string,     // "CA ONLINE"
 *     workShift: string,     // "Check out ca chiều"
 *     date: "10/4",         // "DD/MM"
 *     day: "10",            // Ngày
 *     month: "4",           // Tháng
 *     shiftType: "afternoon", // "morning" hoặc "afternoon"
 *     action: "out"         // "in" hoặc "out"
 *   }
 */
function loadOnlFormData_(formFileId) {
  const ss = SpreadsheetApp.openById(formFileId);
  const sheets = ss.getSheets();
  if (sheets.length === 0) throw new Error('Không tìm thấy sheet trong form file');

  const formSheet = sheets[0]; // Sheet đầu tiên chứa responses
  const lr = formSheet.getLastRow();
  const lc = formSheet.getLastColumn();

  if (lr < 2) {
    Logger.log('Form sheet không có dữ liệu (chỉ có header)');
    return [];
  }

  const values = formSheet.getRange(1, 1, lr, lc).getValues();
  const data = [];

  // Cột A (index 0): Timestamp
  // Cột C (index 2): Họ tên
  // Cột E (index 4): EM CHẤM CÔNG - chứa "CA ONLINE" hoặc "CA OFFLINE"
  // Cột G (index 6): EM CHẤM CÔNG CHO NGÀY NÀO - chứa Date object (ngày ca làm việc)
  // Cột H (index 7): CA LÀM VIỆC CỦA EM - "Check in ca sáng", "Check out ca chiều", ...
  // Quy tắc 24h: timestamp (A) phải cùng ngày với cột G. Check-out qua ngày hôm sau → bỏ qua, không ghi; 01/12 ghi lỗi quên check out, ô 02/12 trống.

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const timestamp = row[0]; // Cột A
    const fullName = String(row[2] || '').trim(); // Cột C
    const checkType = String(row[4] || '').trim(); // Cột E
    const checkDate = row[6]; // Cột G - EM CHẤM CÔNG CHO NGÀY NÀO (Date object)
    const workShift = String(row[7] != null && row[7] !== '' ? row[7] : '').trim(); // Cột H - CA LÀM VIỆC CỦA EM

    // Chỉ lấy các entry có "CA ONLINE"
    if (!checkType.toUpperCase().includes('ONLINE')) continue;
    if (!fullName) continue;

    // Timestamp phải là Date
    if (!timestamp || !(timestamp instanceof Date) || isNaN(timestamp.getTime())) continue;

    // Parse ngày từ cột G (EM CHẤM CÔNG CHO NGÀY NÀO)
    let dateStr = null, dayStr = null, monthStr = null;
    if (checkDate instanceof Date && !isNaN(checkDate.getTime())) {
      const day = checkDate.getDate();
      const month = checkDate.getMonth() + 1;
      // Chuẩn hóa dayStr: đảm bảo là string "1", "2", ..., "31" (không có leading zero)
      dayStr = String(day);
      monthStr = String(month);
      dateStr = `${day}/${month}`;
    }
    if (!dateStr || !dayStr) continue;

    // Quy tắc 24h: timestamp phải cùng ngày với checkDate (cột G). VD: check-in 01/12 8:22 (hợp lệ); check-out 02/12 9:29 cho ca sáng 01/12 → bỏ qua; 01/12 ghi quên check out, ô 02/12 trống.
    try {
      const tz = Session.getScriptTimeZone();
      const tsKey = Utilities.formatDate(timestamp, tz, 'yyyy-MM-dd');
      const cdKey = Utilities.formatDate(checkDate, tz, 'yyyy-MM-dd');
      if (tsKey !== cdKey) {
        Logger.log(`Skip ONL cross-day entry row=${r + 1}: ${fullName} ${checkType} "${workShift}" checkDate=${cdKey} ts=${tsKey}`);
        continue;
      }
    } catch (e) {
      Logger.log(`Skip ONL entry row=${r + 1} due to date-compare error: ${e && e.message ? e.message : e}`);
      continue;
    }

    // Parse ca và action từ workShift (cột H - CA LÀM VIỆC CỦA EM)
    const workShiftLower = workShift.toLowerCase();
    let shiftType = null;
    let action = null;

    if (workShiftLower.includes('sáng') || workShiftLower.includes('sang')) {
      shiftType = 'morning';
    } else if (workShiftLower.includes('chiều') || workShiftLower.includes('chieu')) {
      shiftType = 'afternoon';
    }

    if (workShiftLower.includes('check in') || workShiftLower.includes('checkin')) {
      action = 'in';
    } else if (workShiftLower.includes('check out') || workShiftLower.includes('checkout')) {
      action = 'out';
    }

    if (!shiftType || !action) continue;

    data.push({
      timestamp: timestamp,
      fullName: fullName,
      checkType: checkType,
      workShift: workShift,
      date: dateStr,
      day: dayStr,
      month: monthStr,
      shiftType: shiftType,
      action: action
    });
  }

  Logger.log(`Loaded ${data.length} ONL check-in/out entries from form`);
  return data;
}

/**
 * Load đăng ký ca ONL từ sheet đăng ký ca làm
 * @param {string} scheduleFileId - File ID của sheet đăng ký ca làm
 * @param {Array<string>} sheetNames - Tên các sheet cần đọc
 * @param {Map} nameToEmpMap - Map từ tên (normalized) sang mã nhân viên
 * @return {Map} Map<empCode, Map<dayStr, Set<sessionName>>>
 *   Ví dụ: Map("MH0010", Map("7", Set("morning", "afternoon")))
 */
function loadOnlScheduleRegistrations_(scheduleFileId, sheetNames, nameToEmpMap) {
  const ss = SpreadsheetApp.openById(scheduleFileId);
  const scheduleMap = new Map();
  const unmatched = new Set();

  // Helper để map shift label sang session
  const shiftToSession = (s) => {
    if (!s) return null;
    const t = String(s || '').toUpperCase();
    if (t.startsWith('ST')) return 'morning';
    if (t.startsWith('CT')) return 'afternoon';
    if (t.startsWith('TT') || t.startsWith('TTT') || t.startsWith('T')) return 'evening';
    if (t.includes('SANG')) return 'morning';
    if (t.includes('CHIEU')) return 'afternoon';
    if (t.includes('TOI') || t.includes('TỐI')) return 'evening';
    return 'afternoon';
  };

  sheetNames.forEach(sheetName => {
    const sh = ss.getSheetByName(sheetName);
    if (!sh) {
      Logger.log('Warning: sheet not found ' + sheetName);
      return;
    }

    const lr = sh.getLastRow();
    const lc = sh.getLastColumn();
    const vals = sh.getRange(1, 1, lr, lc).getValues();

    // Tìm header row với date cells và shift row
    let dateRow = null, shiftRow = null, nameCol = null, empCodeCol = null, nameHeaderRow = null;

    for (let r = 0; r < Math.min(10, vals.length); r++) {
      let dateCount = 0;
      for (let c = 0; c < Math.min(50, vals[r].length); c++) {
        const cell = vals[r][c];
        if (cell instanceof Date || /^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(String(cell || ''))) dateCount++;
      }
      if (dateCount >= 1 && dateRow === null) dateRow = r;
    }

    if (dateRow !== null && dateRow + 1 < vals.length) shiftRow = dateRow + 1;

    // Tìm cột mã nhân viên hoặc tên
    for (let r = 0; r <= Math.min(10, vals.length - 1); r++) {
      for (let c = 0; c < Math.min(10, vals[r].length); c++) {
        const h = normalize_(vals[r][c] || '');
        if (!empCodeCol && (h.includes('ma') || h.includes('ma so') || h.includes('mã') || h.includes('mã số') || h.includes('ma nhan vien') || h.includes('mã nhân viên') || h.includes('mã nv'))) {
          empCodeCol = c;
          nameHeaderRow = r;
          break;
        }
        if (!nameCol && (h.includes('cvts') || h.includes('ho ten') || h.includes('họ tên') || h.includes('tên'))) {
          nameCol = c;
          nameHeaderRow = r;
          break;
        }
      }
      if (empCodeCol !== null || nameCol !== null) break;
    }

    if (empCodeCol === null && nameCol === null) {
      Logger.log('Could not find name/code column in ' + sheetName + ' - falling back to column B');
      nameCol = 1;
      nameHeaderRow = 0;
    }

    // Xác định các cột ngày
    const dateCols = []; // array of {colIndex, dayStr, shiftLabel}
    if (dateRow !== null) {
      for (let c = 0; c < vals[dateRow].length; c++) {
        const v = vals[dateRow][c];
        let day = parseDayFromValue_(v);
        // Hỗ trợ format dd/mm/yyyy
        if (!day && /^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(String(v || '').trim())) {
          day = String(Number(String(v).split('/')[0]));
          if (!/^[1-9]$|^[12]\d$|^3[01]$/.test(day)) day = null;
        }
        if (day) {
          const shiftLabel = (shiftRow !== null && vals[shiftRow] && vals[shiftRow][c]) ? String(vals[shiftRow][c] || '') : '';
          dateCols.push({ col: c, dayStr: day, shiftLabel });
        }
      }
    }

    // Duyệt các dòng nhân viên
    const startRow = (nameHeaderRow !== null) ? nameHeaderRow + 1 : 2;
    for (let r = startRow; r < lr; r++) {
      let rawCode = (typeof empCodeCol === 'number') ? String(vals[r][empCodeCol] || '').trim().toUpperCase() : '';
      let empCode = '';

      if (rawCode && /^MH\d{4}$/i.test(rawCode)) {
        empCode = rawCode;
      } else {
        // Fallback to name-based mapping
        const rawName = nameCol !== null ? String(vals[r][nameCol] || '').trim() : '';
        if (!rawName) continue;
        const n = normalize_(rawName.replace(/\(.*\)$/, '').trim());
        empCode = nameToEmpMap.get(n) || '';
        if (!empCode && rawCode) {
          const m = String(rawCode).match(/(MH\d{4})/i);
          if (m) empCode = m[1].toUpperCase();
        }
        if (!empCode) {
          unmatched.add(rawName || rawCode);
          continue;
        }
      }

      // Tìm các ca ONL
      const empSched = new Map();
      let hasOnlRegistration = false;

      for (const dc of dateCols) {
        const c = dc.col;
        const cellVal = String(vals[r][c] || '').trim().toUpperCase();
        // Chỉ lấy các ô có giá trị chính xác là "ONL"
        if (cellVal === 'ONL' || cellVal === 'ONL ▼' || cellVal.startsWith('ONL')) {
          hasOnlRegistration = true;
          const sess = shiftToSession(dc.shiftLabel || '');
          if (!empSched.has(dc.dayStr)) empSched.set(dc.dayStr, new Set());
          empSched.get(dc.dayStr).add(sess);
        }
      }

      if (hasOnlRegistration) {
        scheduleMap.set(empCode, empSched);
      }
    }
  });

  if (unmatched.size) Logger.log('ONL Schedule load: unmatched names count=' + unmatched.size + ' sample=' + Array.from(unmatched).slice(0, 5).join(', '));
  Logger.log(`Loaded ${scheduleMap.size} employees with ONL registrations`);
  return scheduleMap;
}

/**
 * Build name-to-employee-code map từ form data và master employees file
 * @param {Array} formData - Dữ liệu từ Google Form
 * @param {string} masterEmpFileId - File ID của master employees
 * @param {string} masterEmpSheet - Tên sheet chứa danh sách nhân viên
 * @return {Map} Map<normalizedName, empCode>
 */
function buildNameToEmpMapForOnl_(formData, masterEmpFileId, masterEmpSheet) {
  // Lấy tất cả tên unique từ formData
  const uniqueNames = new Set();
  formData.forEach(entry => {
    if (entry.fullName) uniqueNames.add(entry.fullName);
  });

  // Load map từ master employees file
  const nameMap = buildNameToEmpMap_(masterEmpFileId, masterEmpSheet);

  // Tạo map từ tên trong form sang mã nhân viên
  const formToEmpMap = new Map();
  uniqueNames.forEach(name => {
    const normalized = normalize_(name);
    const empCode = nameMap.get(normalized);
    if (empCode) {
      formToEmpMap.set(normalized, empCode);
    } else {
      // Thử fuzzy match
      const tokens = normalized.split(' ');
      for (const [mn, code] of nameMap.entries()) {
        let ok = true;
        for (const t of tokens) {
          if (t && !mn.includes(t)) {
            ok = false;
            break;
          }
        }
        if (ok) {
          formToEmpMap.set(normalized, code);
          break;
        }
      }
    }
  });

  Logger.log(`Built name to emp map: ${formToEmpMap.size} mappings from ${uniqueNames.size} unique names`);
  return formToEmpMap;
}

/**
 * Convert column number to letter (1 -> A, 27 -> AA, etc.)
 */
function columnNumberToLetter_(colNum) {
  let result = '';
  while (colNum > 0) {
    colNum--;
    result = String.fromCharCode(65 + (colNum % 26)) + result;
    colNum = Math.floor(colNum / 26);
  }
  return result;
}

/**
 * Đối chiếu và ghi check in/out ONL vào sheet tổng từ cột DO đến ES
 * @param {string} masterFileId - File ID của sheet tổng
 * @param {string} masterSheetName - Tên sheet tổng
 * @param {Array} formData - Dữ liệu từ Google Form
 * @param {Map} nameToEmpMap - Map từ tên sang mã nhân viên
 * @param {Map} scheduleMap - Map đăng ký ca ONL
 * @param {boolean} dryRun - Nếu true, chỉ preview không ghi
 * @return {Object} Kết quả xử lý
 */
function writeOnlCheckInOutToMaster_(masterFileId, masterSheetName, formData, nameToEmpMap, scheduleMap, dryRun = true) {
  const ss = SpreadsheetApp.openById(masterFileId);
  const masterSh = ss.getSheetByName(masterSheetName);
  if (!masterSh) throw new Error('Không tìm thấy sheet tổng: ' + masterSheetName);

  // Tìm cột DO (cột 119) đến ES (cột 149) - tương ứng với ngày 1-onl đến 31-onl
  // Cột DO = 119, ES = 149 (31 cột)
  const onlStartCol = 119; // DO
  const onlEndCol = 149; // ES
  const onlColCount = onlEndCol - onlStartCol + 1;

  // Đọc header để map ngày -> cột
  const headerRow = 1;
  const header = masterSh.getRange(headerRow, onlStartCol, 1, onlColCount).getValues()[0];
  const dayToCol = new Map(); // dayStr -> colIndex (0-based trong onl block)

  for (let c = 0; c < header.length; c++) {
    const headerVal = String(header[c] || '').trim();
    // Tìm pattern "X-onl" hoặc "X-onl" trong header
    const match = headerVal.match(/^(\d{1,2})-onl$/i);
    if (match) {
      // Chuẩn hóa dayStr: loại bỏ leading zero và đảm bảo là string
      const dayNum = parseInt(match[1], 10);
      if (dayNum >= 1 && dayNum <= 31) {
        const dayStr = String(dayNum); // "1", "2", ..., "29", "30", "31" (không có leading zero)
        dayToCol.set(dayStr, c);
        Logger.log(`  Found column for day ${dayStr}-onl at index ${c} (column ${onlStartCol + c})`);
      }
    }
  }

  Logger.log(`Found ${dayToCol.size} day columns in ONL range (DO-ES, columns ${onlStartCol}-${onlEndCol})`);

  // Build empToRow map
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);

  // Group form data theo empCode và day
  const empDayData = new Map(); // empCode -> Map<dayStr, {morning: {in, out}, afternoon: {in, out}}>

  formData.forEach(entry => {
    const normalizedName = normalize_(entry.fullName);
    const empCode = nameToEmpMap.get(normalizedName);
    if (!empCode) {
      Logger.log(`Warning: Không tìm thấy mã nhân viên cho: ${entry.fullName}`);
      return;
    }

    // Chuẩn hóa dayStr từ entry.day để đảm bảo khớp với dayToCol
    let dayStr = entry.day;
    if (dayStr) {
      const dayNum = parseInt(dayStr, 10);
      if (!isNaN(dayNum) && dayNum >= 1 && dayNum <= 31) {
        dayStr = String(dayNum); // Normalize: "1", "2", ..., "29" (không có leading zero)
      } else {
        Logger.log(`Warning: Invalid dayStr "${entry.day}" for emp ${empCode}, fullName ${entry.fullName}`);
        return;
      }
    } else {
      Logger.log(`Warning: Missing dayStr for emp ${empCode}, fullName ${entry.fullName}`);
      return;
    }

    if (!empDayData.has(empCode)) {
      empDayData.set(empCode, new Map());
    }
    const dayMap = empDayData.get(empCode);

    if (!dayMap.has(dayStr)) {
      dayMap.set(dayStr, {
        morning: { in: null, out: null },
        afternoon: { in: null, out: null }
      });
    }

    const dayData = dayMap.get(dayStr);
    const session = dayData[entry.shiftType];
    if (entry.action === 'in') {
      session.in = entry.timestamp;
    } else if (entry.action === 'out') {
      session.out = entry.timestamp;
    }
  });

  // Đọc dữ liệu hiện có từ cột DO-ES
  const lastEmpRow = masterInfo.lastEmpRow;
  const dataRowCount = lastEmpRow - 1; // Số hàng dữ liệu (bỏ qua hàng header)
  Logger.log(`Reading ONL data: lastEmpRow=${lastEmpRow}, dataRowCount=${dataRowCount}, range: row 2 to ${lastEmpRow}, columns ${onlStartCol} to ${onlEndCol}`);

  const existingOnlData = masterSh.getRange(2, onlStartCol, dataRowCount, onlColCount).getValues();

  // Tạo dữ liệu mới để ghi
  const newOnlData = existingOnlData.map(row => row.slice()); // Copy

  let updatedCount = 0;

  // Duyệt qua tất cả nhân viên có đăng ký ca ONL
  for (const [empCode, daySched] of scheduleMap.entries()) {
    const row1 = masterInfo.empToRow.get(empCode);
    if (!row1) {
      Logger.log(`Warning: Không tìm thấy nhân viên ${empCode} trong sheet tổng`);
      continue;
    }

    // Bỏ qua hàng 1 (header) - chỉ xử lý từ hàng 2 trở đi
    if (row1 === 1) {
      Logger.log(`Warning: Skipping row 1 (header) for emp ${empCode}`);
      continue;
    }

    // row1 là số hàng 1-based trong sheet (ví dụ: hàng 2, 3, 4...)
    // dataIndex là index 0-based trong array (hàng 2 -> index 0, hàng 3 -> index 1...)
    const dataIndex = row1 - 2; // Hàng 2 -> 0, hàng 3 -> 1, ...

    if (dataIndex < 0 || dataIndex >= newOnlData.length) {
      Logger.log(`Warning: ${empCode} row ${row1} -> dataIndex ${dataIndex} out of range (0-${newOnlData.length - 1})`);
      continue;
    }

    // Duyệt qua từng ngày có đăng ký ca ONL
    for (const [dayStrRaw, sessions] of daySched.entries()) {
      // Chuẩn hóa dayStr từ schedule để đảm bảo khớp với dayToCol
      let dayStr = dayStrRaw;
      const dayNum = parseInt(dayStrRaw, 10);
      if (!isNaN(dayNum) && dayNum >= 1 && dayNum <= 31) {
        dayStr = String(dayNum); // Normalize: "1", "2", ..., "29" (không có leading zero)
      } else {
        Logger.log(`Warning: Invalid dayStr "${dayStrRaw}" in schedule for emp ${empCode}`);
        continue;
      }

      const colIndex = dayToCol.get(dayStr);
      if (colIndex === undefined) {
        Logger.log(`Warning: Không tìm thấy cột cho ngày ${dayStr}-onl (raw: "${dayStrRaw}") cho emp ${empCode}`);
        Logger.log(`  Available day columns: ${Array.from(dayToCol.keys()).sort((a, b) => parseInt(a) - parseInt(b)).join(', ')}`);
        continue;
      }

      const dayData = empDayData.get(empCode)?.get(dayStr);
      if (!dayData) {
        // Không có dữ liệu check in/out cho ngày này từ form
        Logger.log(`Debug: Emp ${empCode}, day ${dayStr}: Có đăng ký ca ONL nhưng không có dữ liệu form (có thể quên check-in/out hoặc chưa submit form)`);
        continue;
      }

      // Tạo text để ghi vào ô từ dữ liệu Google Form (check in/out thực tế)
      // Format: "onl ca sáng check in HH:mm\nonl ca sáng check out HH:mm"
      const notes = [];
      sessions.forEach(sessionName => {
        const session = dayData[sessionName];
        if (session && (session.in || session.out)) {
          const sessionLabel = sessionName === 'morning' ? 'ca sáng' : 'ca chiều';
          if (session.in) {
            const timeStr = Utilities.formatDate(session.in, Session.getScriptTimeZone(), 'HH:mm');
            notes.push(`onl ${sessionLabel} check in ${timeStr}`);
          }
          if (session.out) {
            const timeStr = Utilities.formatDate(session.out, Session.getScriptTimeZone(), 'HH:mm');
            notes.push(`onl ${sessionLabel} check out ${timeStr}`);
          }
        }
      });

      if (notes.length > 0) {
        // GHI ĐÈ nội dung cũ bằng dữ liệu mới từ Google Form
        // (Không append để tránh lặp lại dữ liệu cũ)
        const newValue = notes.join('\n');
        const actualCol = onlStartCol + colIndex;
        // Convert column number to letter (A=1, Z=26, AA=27, ...)
        const colLetter = columnNumberToLetter_(actualCol);
        const existing = String(newOnlData[dataIndex][colIndex] || '').trim();

        Logger.log(`  Updating ${empCode} row ${row1}, column ${colLetter}${actualCol} (${dayStr}-onl)`);
        Logger.log(`    Source: Google Form check-in/out data`);
        Logger.log(`    DayStr normalized: "${dayStr}" (raw from schedule: "${dayStrRaw}")`);
        if (existing) {
          Logger.log(`    Old value (will be replaced): "${existing.substring(0, 80)}${existing.length > 80 ? '...' : ''}"`);
        }
        Logger.log(`    New value: "${newValue}"`);

        newOnlData[dataIndex][colIndex] = newValue;
        updatedCount++;
      }
    }
  }

  if (dryRun) {
    Logger.log(`PREVIEW (dryRun) - Sẽ cập nhật ${updatedCount} ô trong cột DO-ES`);
    return { updatedCells: updatedCount, dryRun: true };
  }

  // Ghi vào sheet
  if (updatedCount > 0) {
    Logger.log(`Writing ${dataRowCount} rows x ${onlColCount} columns to range: row 2-${lastEmpRow}, columns ${onlStartCol}-${onlEndCol}`);
    const range = masterSh.getRange(2, onlStartCol, dataRowCount, onlColCount);

    // Kiểm tra một vài giá trị trước khi ghi
    let sampleCount = 0;
    for (let r = 0; r < Math.min(5, newOnlData.length); r++) {
      for (let c = 0; c < newOnlData[r].length; c++) {
        const val = String(newOnlData[r][c] || '').trim();
        if (val && val.includes('onl')) {
          const colLetter = columnNumberToLetter_(onlStartCol + c);
          Logger.log(`  Sample: Row ${r + 2}, Col ${colLetter}${onlStartCol + c}: "${val.substring(0, 80)}${val.length > 80 ? '...' : ''}"`);
          sampleCount++;
          if (sampleCount >= 3) break;
        }
      }
      if (sampleCount >= 3) break;
    }

    // Đảm bảo format là text để tránh bị format number/date ghi đè
    range.setNumberFormat('@'); // @ = text format
    range.setValues(newOnlData);

    // Force flush để đảm bảo dữ liệu được ghi
    SpreadsheetApp.flush();

    Logger.log(`Đã cập nhật ${updatedCount} ô trong cột DO-ES (đã ghi ${dataRowCount} hàng x ${onlColCount} cột)`);
    Logger.log(`Sheet: ${masterSheetName}, File ID: ${masterFileId}`);
    Logger.log(`URL: https://docs.google.com/spreadsheets/d/${masterFileId}/edit#gid=${masterSh.getSheetId()}`);

    // Verify sau khi ghi (chỉ lấy một vài ô để kiểm tra)
    if (!dryRun) {
      let verifyCount = 0;
      const verifyDetails = [];
      for (const [empCode, daySched] of scheduleMap.entries()) {
        if (verifyCount >= 5) break;
        const row1 = masterInfo.empToRow.get(empCode);
        if (!row1) continue;
        const dataIndex = row1 - 2;
        if (dataIndex < 0 || dataIndex >= newOnlData.length) continue;

        for (const [dayStr, sessions] of daySched.entries()) {
          const colIndex = dayToCol.get(dayStr);
          if (colIndex === undefined) continue;
          const actualCol = onlStartCol + colIndex;
          const colLetter = columnNumberToLetter_(actualCol);

          // Đọc lại từ sheet để verify
          const writtenValue = masterSh.getRange(row1, actualCol).getValue();
          const expectedValue = newOnlData[dataIndex][colIndex];

          if (writtenValue || expectedValue) {
            const writtenStr = String(writtenValue || '');
            const expectedStr = String(expectedValue || '');
            Logger.log(`  Verify ${verifyCount + 1}: ${empCode} row ${row1}, col ${colLetter}${actualCol} (${dayStr}-onl)`);
            Logger.log(`    Expected: "${expectedStr.substring(0, 100)}${expectedStr.length > 100 ? '...' : ''}"`);
            Logger.log(`    Written:  "${writtenStr.substring(0, 100)}${writtenStr.length > 100 ? '...' : ''}"`);
            Logger.log(`    Match: ${writtenStr.trim() === expectedStr.trim() ? 'YES' : 'NO'}`);

            verifyDetails.push({
              empCode: empCode,
              row: row1,
              col: `${colLetter}${actualCol}`,
              day: dayStr,
              match: writtenStr.trim() === expectedStr.trim()
            });
            verifyCount++;
            break;
          }
        }
      }

      // Tổng kết verification
      const matchCount = verifyDetails.filter(v => v.match).length;
      Logger.log(`Verification: ${matchCount}/${verifyDetails.length} cells match`);

      if (verifyDetails.length > 0) {
        Logger.log(`Sample cells to check manually:`);
        verifyDetails.slice(0, 3).forEach(v => {
          Logger.log(`  - ${v.empCode} at ${v.col}${v.row} (day ${v.day}-onl)`);
        });
      }
    }
  } else {
    Logger.log('Không có ô nào cần cập nhật');
  }

  return { updatedCells: updatedCount, dryRun: false };
}

/**
 * Hàm chính để xử lý check in/out ONL từ Google Form
 * @param {boolean} dryRun - Nếu true, chỉ preview không ghi
 */
function processOnlCheckInOut(dryRun = true) {
  // ====== CONFIG ======
  const FORM_FILE_ID = '1_mmyOMrX8cOW3bEqt6HxE5B7A0wxH5ud_SVyZEMMDQE';
  const SCHEDULE_FILE_ID = '1oKFAsC-mhAtA_yzHk8TwC3k5cCYzdNKFTgYSxfbDsSo';
  const SCHEDULE_SHEETS = ['LỊCH LÀM T12/2025', 'PAGE LỄ TÂN - LỊCH LÀM 2025'];
  const MASTER_EMP_FILE_ID = '1_szrWl2X-6Kcp7lpdl4HmBo7uciLqDGO-VWq1uie3HY';
  const MASTER_EMP_SHEET = 'MÃ SỐ NHÂN VIÊN';
  const MASTER_FILE_ID = '1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4';
  const MASTER_SHEET_NAME = 'Chấm công th12/2025';

  Logger.log('1) Loading ONL form data...');
  const formData = loadOnlFormData_(FORM_FILE_ID);
  if (formData.length === 0) {
    Logger.log('Không có dữ liệu form để xử lý');
    return { success: false, message: 'Không có dữ liệu form' };
  }

  Logger.log('2) Building name to employee map...');
  const nameToEmpMap = buildNameToEmpMapForOnl_(formData, MASTER_EMP_FILE_ID, MASTER_EMP_SHEET);

  Logger.log('3) Loading ONL schedule registrations...');
  const scheduleMap = loadOnlScheduleRegistrations_(SCHEDULE_FILE_ID, SCHEDULE_SHEETS, nameToEmpMap);

  Logger.log('4) Writing to master sheet...');
  const result = writeOnlCheckInOutToMaster_(MASTER_FILE_ID, MASTER_SHEET_NAME, formData, nameToEmpMap, scheduleMap, dryRun);

  try {
    const ui = SpreadsheetApp.getUi();
    if (dryRun) {
      ui.alert(`Preview: Sẽ cập nhật ${result.updatedCells} ô trong cột DO-ES\n\nChạy processOnlCheckInOutCommit() để ghi vào sheet.`);
    } else {
      ui.alert(`Hoàn thành! Đã cập nhật ${result.updatedCells} ô trong cột DO-ES.`);
    }
  } catch (e) {
    Logger.log(`Alert skipped (no UI available)`);
  }

  return result;
}

/**
 * Helper function để ghi trực tiếp vào sheet (không preview)
 */
function processOnlCheckInOutCommit() {
  Logger.log('Running commit: processOnlCheckInOut(dryRun=false)');
  return processOnlCheckInOut(false);
}

// ==================== XỬ LÝ LỖI ONL TỪ DỮ LIỆU TRONG SHEET (KHÔNG ĐỐI CHIẾU ĐĂNG KÝ CA) ====================

/**
 * Parse nội dung ô ONL: trích times (HH:mm) và loại ca từ chuỗi như "08:32\n12:25\n16:50\nonl 2 ca"
 * @param {*} cell - Giá trị ô (string/Date/number)
 * @return {{ times: string[], shiftType: 'morning'|'afternoon'|'two' } | null} - null nếu không parse được
 */
function parseOnlCell_(cell) {
  const times = extractTimesFromCell_(cell);
  if (!times || times.length === 0) return null;

  const text = String(cell || '').trim().toLowerCase();
  // Xác định loại ca từ phần không phải HH:mm (vd: "onl 2 ca", "onl ca sáng", "onl ca chiều")
  if (/onl\s*2\s*ca|2\s*ca|onl\s*2ca/.test(text)) return { times, shiftType: 'two' };
  if (/sáng|sang/.test(text) && !/chiều|chieu/.test(text)) return { times, shiftType: 'morning' };
  if (/chiều|chieu/.test(text)) return { times, shiftType: 'afternoon' };
  // Mặc định: 4 mốc => 2 ca, 2 mốc => phân biệt theo giờ (trước 12h = sáng, sau = chiều)
  const cutoff = timeStrToMinutes_('12:00') || 720;
  if (times.length >= 4) return { times, shiftType: 'two' };
  if (times.length === 2) {
    const m = timeStrToMinutes_(times[0]);
    return { times, shiftType: (m !== null && m < cutoff) ? 'morning' : 'afternoon' };
  }
  if (times.length === 3) return { times, shiftType: 'two' }; // 3 mốc = 2 ca thiếu 1 (vd: quên check in chiều)
  return { times, shiftType: 'morning' };
}

/**
 * Từ (times, shiftType) và giờ bắt đầu ca, suy ra morning/afternoon in, out, lateMinutes
 * - "onl 2 ca" + 4 times: [in_sáng, out_sáng, in_chiều, out_chiều]
 * - "onl 2 ca" + 3 times: [in_sáng, out_sáng, out_chiều] → thiếu in_chiều
 * - "onl ca sáng" + 2 times: [in, out]
 * - "onl ca chiều" + 2 times: [in, out]
 */
function buildOnlSessionsFromParsed_(parsed, morningStartMin, afternoonStartMin) {
  if (!parsed || !parsed.times || parsed.times.length === 0) return [];
  const t = parsed.times.slice().sort((a, b) => (timeStrToMinutes_(a) || 0) - (timeStrToMinutes_(b) || 0));
  const out = [];

  const late = (inStr, startMin) => {
    const m = timeStrToMinutes_(inStr);
    if (m === null || startMin === null) return 0;
    return Math.max(0, m - startMin);
  };

  if (parsed.shiftType === 'morning') {
    const inStr = t[0];
    const outStr = t.length >= 2 ? t[1] : null;
    out.push({
      name: 'morning',
      in: inStr,
      out: outStr,
      lateMinutes: late(inStr, morningStartMin)
    });
    return out;
  }
  if (parsed.shiftType === 'afternoon') {
    const inStr = t[0];
    const outStr = t.length >= 2 ? t[1] : null;
    out.push({
      name: 'afternoon',
      in: inStr,
      out: outStr,
      lateMinutes: late(inStr, afternoonStartMin)
    });
    return out;
  }
  // two
  const cutoff = timeStrToMinutes_('12:00') || 720;
  
  if (parsed.times.length >= 4) {
    // Với "onl 2 ca" và 4 times: thứ tự TRONG Ô = [in_sáng, out_sáng, in_chiều, out_chiều].
    // Không sort theo giờ: check-out sáng có thể muộn (vd 23:30), check-in chiều 13:10 → sort sẽ gán sai (23:30 thành in chiều → báo trễ 615 phút). Dùng thứ tự gốc.
    const order = parsed.times.slice(0, 4);
    const morningIn = order[0];
    const morningOut = order[1];
    const afternoonIn = order[2];
    const afternoonOut = order[3];
    
    out.push(
      { name: 'morning', in: morningIn, out: morningOut, lateMinutes: late(morningIn, morningStartMin) },
      { name: 'afternoon', in: afternoonIn, out: afternoonOut, lateMinutes: late(afternoonIn, afternoonStartMin) }
    );
  } else if (t.length === 3) {
    // Với 3 times và "onl 2 ca", cần phân tích dựa trên cutoff (12:00)
    // Phân loại times: trước 12:00 = ca sáng, sau 12:00 = ca chiều
    // Lưu ý: times gần 12:00 (11:00-13:00) có thể là out_sáng hoặc in_chiều
    const timesWithMin = t.map(time => ({ time, min: timeStrToMinutes_(time) })).filter(x => x.min !== null);
    const morningTimes = timesWithMin.filter(x => x.min < cutoff);
    const afternoonTimes = timesWithMin.filter(x => x.min >= cutoff);
    
    // Xử lý ca sáng
    if (morningTimes.length === 0) {
      // Không có times trước 12:00 → thiếu cả in và out sáng
      out.push({ name: 'morning', in: null, out: null, lateMinutes: 0 });
    } else if (morningTimes.length === 1) {
      // Có 1 time trước 12:00 → có thể là in hoặc out sáng
      const m = morningTimes[0].min;
      // Nếu time gần giờ bắt đầu ca sáng (08:30) → có thể là in, ngược lại là out
      if (m < morningStartMin + 60) { // Trước 09:30 → có thể là in
        out.push({ name: 'morning', in: morningTimes[0].time, out: null, lateMinutes: late(morningTimes[0].time, morningStartMin) });
      } else {
        out.push({ name: 'morning', in: null, out: morningTimes[0].time, lateMinutes: 0 });
      }
    } else if (morningTimes.length === 2) {
      // Có 2 times trước 12:00 → in và out sáng
      out.push({ 
        name: 'morning', 
        in: morningTimes[0].time, 
        out: morningTimes[1].time, 
        lateMinutes: late(morningTimes[0].time, morningStartMin) 
      });
    } else {
      // Có > 2 times trước 12:00 → không hợp lý, xử lý như 2 times đầu
      out.push({ 
        name: 'morning', 
        in: morningTimes[0].time, 
        out: morningTimes[1].time, 
        lateMinutes: late(morningTimes[0].time, morningStartMin) 
      });
    }
    
    // Xử lý ca chiều
    // Lưu ý: Nếu có times gần 12:00 (11:30-12:30), có thể là out_sáng hoặc in_chiều
    // Ưu tiên: nếu có 3 times sau 12:00, times đầu tiên có thể là out_sáng (nếu quá sớm)
    if (afternoonTimes.length === 0) {
      // Không có times sau 12:00 → thiếu cả in và out chiều
      out.push({ name: 'afternoon', in: null, out: null, lateMinutes: 0 });
    } else if (afternoonTimes.length === 1) {
      // Có 1 time sau 12:00 → có thể là in hoặc out chiều
      const m = afternoonTimes[0].min;
      // Nếu time gần giờ bắt đầu ca chiều (13:15) → có thể là in, ngược lại là out
      if (m < afternoonStartMin + 60) { // Trước 14:15 → có thể là in
        out.push({ name: 'afternoon', in: afternoonTimes[0].time, out: null, lateMinutes: late(afternoonTimes[0].time, afternoonStartMin) });
      } else {
        out.push({ name: 'afternoon', in: null, out: afternoonTimes[0].time, lateMinutes: 0 });
      }
    } else if (afternoonTimes.length === 2) {
      // Có 2 times sau 12:00 → in và out chiều
      out.push({ 
        name: 'afternoon', 
        in: afternoonTimes[0].time, 
        out: afternoonTimes[1].time, 
        lateMinutes: late(afternoonTimes[0].time, afternoonStartMin) 
      });
    } else {
      // Có 3 times sau 12:00 → có thể là [out_sáng (gần 12:00), in_chiều, out_chiều]
      // Kiểm tra: nếu times[0] quá sớm (< 13:00) và times[1] gần giờ bắt đầu ca chiều (13:15), thì times[0] có thể là out_sáng
      const firstTimeMin = afternoonTimes[0].min;
      const secondTimeMin = afternoonTimes[1].min;
      const thirdTimeMin = afternoonTimes[2].min;
      const cutoff1300 = timeStrToMinutes_('13:00') || 780; // 13:00 = 780 phút
      
      // Nếu times[0] < 13:00 và times[1] gần giờ bắt đầu ca chiều (13:15), thì times[0] có thể là out_sáng
      if (firstTimeMin < cutoff1300 && secondTimeMin >= afternoonStartMin - 30 && secondTimeMin <= afternoonStartMin + 60) {
        // times[0] = out_sáng, times[1] = in_chiều, times[2] = out_chiều
        out.push({ name: 'morning', in: null, out: afternoonTimes[0].time, lateMinutes: 0 });
        out.push({ 
          name: 'afternoon', 
          in: afternoonTimes[1].time, 
          out: afternoonTimes[2].time, 
          lateMinutes: late(afternoonTimes[1].time, afternoonStartMin) 
        });
      } else {
        // Xử lý như 2 times đầu là in và out chiều, times[3] bỏ qua (có thể là lỗi)
        out.push({ 
          name: 'afternoon', 
          in: afternoonTimes[0].time, 
          out: afternoonTimes[1].time, 
          lateMinutes: late(afternoonTimes[0].time, afternoonStartMin) 
        });
      }
    }
  } else if (t.length === 2) {
    // Với 2 times và "onl 2 ca", phân tích dựa trên cutoff
    const t0Min = timeStrToMinutes_(t[0]);
    const t1Min = timeStrToMinutes_(t[1]);
    
    if (t0Min !== null && t1Min !== null) {
      if (t0Min < cutoff && t1Min < cutoff) {
        // Cả 2 times đều trước 12:00 → ca sáng
        out.push({ name: 'morning', in: t[0], out: t[1], lateMinutes: late(t[0], morningStartMin) });
        out.push({ name: 'afternoon', in: null, out: null, lateMinutes: 0 });
      } else if (t0Min >= cutoff && t1Min >= cutoff) {
        // Cả 2 times đều sau 12:00 → ca chiều
        out.push({ name: 'morning', in: null, out: null, lateMinutes: 0 });
        out.push({ name: 'afternoon', in: t[0], out: t[1], lateMinutes: late(t[0], afternoonStartMin) });
      } else {
        // 1 time trước, 1 time sau → có thể là out_sáng và in_chiều
        out.push({ name: 'morning', in: null, out: t[0], lateMinutes: 0 });
        out.push({ name: 'afternoon', in: t[1], out: null, lateMinutes: late(t[1], afternoonStartMin) });
      }
    } else {
      // Fallback: giả định là ca sáng
      out.push({ name: 'morning', in: t[0], out: t[1], lateMinutes: late(t[0], morningStartMin) });
    }
  } else if (t.length === 1) {
    const m = timeStrToMinutes_(t[0]);
    if (m !== null && m < cutoff) {
      out.push({ name: 'morning', in: t[0], out: null, lateMinutes: late(t[0], morningStartMin) });
      out.push({ name: 'afternoon', in: null, out: null, lateMinutes: 0 });
    } else {
      out.push({ name: 'morning', in: null, out: null, lateMinutes: 0 });
      out.push({ name: 'afternoon', in: t[0], out: null, lateMinutes: late(t[0], afternoonStartMin) });
    }
  }
  return out;
}

/**
 * Phân tích lỗi ONL chỉ từ dữ liệu trong các cột "X-onl", không đối chiếu đăng ký ca.
 * Duyệt mọi nhân viên, mọi cột X-onl; ô nào có nội dung thì parse và phát hiện quên check in/out + trễ.
 * @param {Object} masterInfo - Thông tin master sheet (cần masterSh, header, lastEmpRow, empToRow, empToRole)
 * @param {Object} cfg - Config (morningStart, afternoonStart, lateThreshold)
 * @param {number} month - Tháng
 * @param {string} mode - 'both' | 'late' | 'missing'
 * @return {Object} { changes: Map<r0, {notes, onlErrorCount}>, problematicCells: Array }
 */
function prepareOnlAttendanceChangesFromSheet_(masterInfo, cfg, month, mode) {
  const changes = new Map();
  const problematicCells = [];
  const runLate = (mode === 'both' || mode === 'late');
  const runMissing = (mode === 'both' || mode === 'missing');

  const humanForSession = (s) => (s === 'morning' ? 'ca sáng' : s === 'afternoon' ? 'ca chiều' : 'ca');

  const onlStartCol = 119;
  const onlEndCol = 149;
  const onlColCount = onlEndCol - onlStartCol + 1;
  const header = masterInfo.header || [];
  const onlDayToCol = new Map();

  for (let c = onlStartCol - 1; c < Math.min(onlStartCol - 1 + onlColCount, header.length); c++) {
    const h = String(header[c] || '').trim();
    const match = h.match(/^(\d{1,2})-onl$/i);
    if (match) onlDayToCol.set(match[1], c + 1);
  }

  Logger.log(`Found ${onlDayToCol.size} ONL day columns (X-onl) — không dùng đăng ký ca, chỉ từ data ô`);

  const lastEmpRow = masterInfo.lastEmpRow;
  if (onlDayToCol.size === 0 || !masterInfo.masterSh || lastEmpRow < 2) {
    return { changes, problematicCells };
  }

  const dataRowCount = lastEmpRow - 1;
  const onlDataBlock = masterInfo.masterSh.getRange(2, onlStartCol, dataRowCount, onlColCount).getValues();

  // Duyệt theo hàng (mỗi hàng = một nhân viên theo thứ tự trong sheet)
  // LƯU Ý: onlDataBlock được đọc từ hàng 2, nên:
  // - onlDataBlock[0] = hàng 2 trong sheet
  // - onlDataBlock[1] = hàng 3 trong sheet
  // - onlDataBlock[2] = hàng 4 trong sheet
  // Vậy nếu r1 = 4 (hàng 4 trong sheet), thì r0 = r1 - 2 = 2 (đúng với onlDataBlock[2])
  const empRows = []; // [ { r0, empCode } ]
  for (const [empCode, r1] of masterInfo.empToRow.entries()) {
    // Bỏ qua hàng 1 (header)
    if (r1 === 1) continue;
    const r0 = r1 - 2; // Hàng 2 -> index 0, hàng 3 -> index 1, hàng 4 -> index 2, ...
    if (r0 < 0 || r0 >= onlDataBlock.length) continue;
    empRows.push({ r0, empCode });
  }

  for (const { r0, empCode } of empRows) {
    let notesForDetail = [];
    let onlErrorCount = 0;
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(empCode) : undefined;
    const scheduleTemplate = getEmployeeSchedule_(empCode, cfg, role);
    
    // Kiểm tra và fallback về default nếu schedule không có morning/afternoon (trường hợp reception)
    const morningStart = scheduleTemplate.morning?.start || cfg.morningStart || SPECIAL_SCHEDULES.default.morningStart;
    const afternoonStart = scheduleTemplate.afternoon?.start || cfg.afternoonStart || SPECIAL_SCHEDULES.default.afternoonStart;
    const morningStartMin = timeStrToMinutes_(morningStart);
    const afternoonStartMin = timeStrToMinutes_(afternoonStart);

    for (const [dayStr, col1] of onlDayToCol.entries()) {
      const c0 = col1 - onlStartCol;
      if (c0 < 0 || c0 >= onlColCount) continue;

      const cellValue = onlDataBlock[r0][c0];
      
      // Kiểm tra chặt chẽ: chỉ xử lý khi ô có dữ liệu thực sự
      // Bỏ qua nếu ô trống, null, undefined
      if (cellValue === null || cellValue === undefined || cellValue === '') continue;
      
      // Kiểm tra: nếu cellValue là số, phải là time serial hợp lệ
      // Google Sheets time serial: 0 = 00:00:00, 1 = 24:00:00
      // Bỏ qua nếu là 0 (có thể là ô trống được parse thành 0) hoặc NaN
      if (typeof cellValue === 'number') {
        if (cellValue === 0 || isNaN(cellValue) || !isFinite(cellValue)) continue;
      }
      
      // Kiểm tra string: bỏ qua nếu chỉ có khoảng trắng
      const cellValueStr = String(cellValue || '').trim();
      if (!cellValueStr) continue;
      
      // Parse và kiểm tra kỹ: phải có times thực sự
      const parsed = parseOnlCell_(cellValue);
      if (!parsed || !parsed.times || !Array.isArray(parsed.times) || parsed.times.length === 0) continue;
      
      // Kiểm tra thêm: các times phải là string hợp lệ (HH:mm format)
      const validTimes = parsed.times.filter(t => {
        if (typeof t !== 'string') return false;
        const timeMatch = t.match(/^([01]?\d|2[0-3]):[0-5]\d$/);
        return timeMatch !== null;
      });
      if (validTimes.length === 0) continue;
      
      // Cập nhật parsed.times với chỉ các times hợp lệ
      parsed.times = validTimes;

      const sessions = buildOnlSessionsFromParsed_(parsed, morningStartMin, afternoonStartMin);
      if (!sessions || sessions.length === 0) continue;
      
      const dayStrFormatted = `${parseInt(dayStr, 10)}/${month}`;

      for (const session of sessions) {
        // Chỉ xử lý session có dữ liệu thực sự (có ít nhất in hoặc out)
        if (!session || (session.in === null && session.out === null)) continue;
        
        const human = humanForSession(session.name);

        if (runMissing) {
          // Chỉ xử lý quên check in/out
          if (session.in == null && session.out != null) {
            notesForDetail.push(`- Quên check in ${human} ${dayStrFormatted} (ONL)`);
            onlErrorCount++;
          } else if (session.in != null && session.out == null) {
            notesForDetail.push(`- Quên check out ${human} ${dayStrFormatted} (ONL)`);
            onlErrorCount++;
          } else if (session.in && (typeof session.lateMinutes === 'number' && session.lateMinutes > 0)) {
            // Trễ >= 30 phút: quy thành quên check in (chỉ xử lý trong mode missing). Đồng bộ với highlight: không bắt buộc có session.out.
            const lateMin = Math.round(session.lateMinutes);
            const threshold = cfg.lateThreshold || 30;
            if (lateMin >= threshold) {
              notesForDetail.push(`- Check in trễ từ 30 phút trở lên (${lateMin} phút) ${human} ${dayStrFormatted} (ONL)`);
              onlErrorCount++;
            }
          }
        }

        if (runLate && session.in && (typeof session.lateMinutes === 'number' && session.lateMinutes > 0)) {
          // Chỉ xử lý trễ < 30 phút (trễ >= 30 đã được xử lý ở mode missing). Đồng bộ với highlight: không bắt buộc có session.out.
          const lateMin = Math.round(session.lateMinutes);
          const threshold = cfg.lateThreshold || 30;
          
          // Trễ < 30 phút là lỗi TRỄ, ghi cột S
          if (lateMin > 0 && lateMin < threshold) {
            notesForDetail.push(`- Check in trễ dưới 30 phút (${lateMin} phút) ${human} ${dayStrFormatted} (ONL)`);
          }
        }
      }
    }

    if (notesForDetail.length) {
      changes.set(r0, { notes: notesForDetail, onlErrorCount });
    }
  }

  return { changes, problematicCells };
}

/**
 * Phân tích dữ liệu ONL form để phát hiện lỗi quên check in/out và trễ
 * @param {Array} formData - Dữ liệu từ Google Form (loadOnlFormData_)y
 * @param {Map} nameToEmpMap - Map từ tên sang mã nhân viên
 * @param {Map} scheduleMap - Map đăng ký ca ONL (loadOnlScheduleRegistrations_)
 * @param {Object} cfg - Config (morningStart, afternoonStart, lateThreshold)
 * @param {number} month - Tháng
 * @param {string} mode - 'both' | 'late' | 'missing'
 * @return {Object} { changes: Map<r0, {notes, onlErrorCount}>, problematicCells: Array }
 */
function prepareOnlAttendanceChanges_(formData, nameToEmpMap, scheduleMap, masterInfo, cfg, month, mode) {
  const changes = new Map();
  const problematicCells = [];
  const normalizedMode = (String(mode || 'both').toLowerCase());
  const runLate = (normalizedMode === 'both' || normalizedMode === 'late');
  const runMissing = (normalizedMode === 'both' || normalizedMode === 'missing');

  // Helper để convert session name sang human-readable
  const humanForSession = (sess) => {
    if (!sess) return 'ca';
    if (sess === 'morning') return 'ca sáng';
    if (sess === 'afternoon') return 'ca chiều';
    if (sess === 'evening') return 'ca tối';
    return 'ca';
  };

  // Group form data theo empCode và day
  const empDayData = new Map(); // Map<empCode, Map<dayStr, {morning: {in, out}, afternoon: {in, out}}>>

  for (const entry of formData) {
    const normalizedName = normalize_(entry.fullName);
    const empCode = nameToEmpMap.get(normalizedName);
    if (!empCode) continue;

    const dayStr = entry.day;
    if (!dayStr) continue;

    if (!empDayData.has(empCode)) empDayData.set(empCode, new Map());
    const dayMap = empDayData.get(empCode);
    if (!dayMap.has(dayStr)) {
      dayMap.set(dayStr, {
        morning: { in: null, out: null },
        afternoon: { in: null, out: null },
        evening: { in: null, out: null }
      });
    }

    const session = dayMap.get(dayStr)[entry.shiftType];
    if (!session) continue;

    // Lưu timestamp vào in hoặc out
    if (entry.action === 'in') {
      session.in = entry.timestamp;
    } else if (entry.action === 'out') {
      session.out = entry.timestamp;
    }
  }

  // Duyệt qua tất cả nhân viên có đăng ký ca ONL
  for (const [empCode, daySchedMap] of scheduleMap.entries()) {
    const r1 = masterInfo.empToRow.get(empCode);
    if (!r1) continue; // Không có trong master sheet
    const r0 = r1 - 1;

    let notesForDetail = [];
    let onlErrorCount = 0;

    // Lấy schedule template để tính lateMinutes
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(empCode) : undefined;
    const scheduleTemplate = getEmployeeSchedule_(empCode, cfg, role);

    // Duyệt qua từng ngày có đăng ký ca ONL
    for (const [dayStr, registeredSessions] of daySchedMap.entries()) {
      const dayData = empDayData.get(empCode);
      const sessions = dayData && dayData.get(dayStr) ? dayData.get(dayStr) : {
        morning: { in: null, out: null },
        afternoon: { in: null, out: null },
        evening: { in: null, out: null }
      };

      // Duyệt qua từng ca đã đăng ký
      for (const sessionName of registeredSessions) {
        if (sessionName === '_problematic' || sessionName === '_timesCount') continue;

        const session = sessions[sessionName];
        if (!session) continue;

        const human = humanForSession(sessionName);
        const dayNum = parseInt(dayStr, 10);
        const dayStrFormatted = `${dayNum}/${month}`;

        // Tính lateMinutes trước để dùng cho cả missing và late mode. Đồng bộ với highlight: tính khi có session.in (không bắt buộc session.out).
        let lateMinutes = 0;
        if (session.in) {
          const checkInTime = session.in;
          const checkInMin = checkInTime.getHours() * 60 + checkInTime.getMinutes();

          let sessionStartMin = null;
          if (sessionName === 'morning') {
            const morningStart = scheduleTemplate.morning?.start || cfg.morningStart || SPECIAL_SCHEDULES.default.morningStart;
            sessionStartMin = timeStrToMinutes_(morningStart);
          } else if (sessionName === 'afternoon') {
            const afternoonStart = scheduleTemplate.afternoon?.start || cfg.afternoonStart || SPECIAL_SCHEDULES.default.afternoonStart;
            sessionStartMin = timeStrToMinutes_(afternoonStart);
          }

          if (sessionStartMin !== null && checkInMin > sessionStartMin) {
            lateMinutes = checkInMin - sessionStartMin;
          }
        }
        const lateMinutesRounded = Math.round(lateMinutes);
        const threshold = cfg.lateThreshold || 30;

        // 1) Xử lý QUÊN CHECK IN/OUT (tùy theo mode)
        if (runMissing) {
          if (!session.in && !session.out) {
            // Vắng hoàn toàn - không tính vào lỗi quên check in/out
            continue;
          }
          if (!session.in && session.out) {
            // Quên check in nhưng có check out
            notesForDetail.push(`- Quên check in ${human} ${dayStrFormatted} (ONL)`);
            onlErrorCount++;
          } else if (session.in && !session.out) {
            // Có check in nhưng quên check out
            notesForDetail.push(`- Quên check out ${human} ${dayStrFormatted} (ONL)`);
            onlErrorCount++;
          } else if (session.in && lateMinutesRounded > threshold) {
            // Trễ >= 30 phút: quy thành quên check in (chỉ xử lý trong mode missing). Đồng bộ với highlight: không bắt buộc session.out.
            notesForDetail.push(`- Check in trễ từ 30 phút trở lên (${lateMinutesRounded} phút) ${human} ${dayStrFormatted} (ONL)`);
            onlErrorCount++;
          }
        }

        // 2) Xử lý TRỄ CHECK-IN (tùy theo mode). Đồng bộ với highlight: không bắt buộc session.out.
        if (runLate && session.in && lateMinutesRounded > 0 && lateMinutesRounded <= threshold) {
          // Trễ <= 30 phút (bao gồm cả = 30 phút) là lỗi TRỄ, ghi cột S
          notesForDetail.push(`- Check in trễ dưới hoặc bằng 30 phút (${lateMinutesRounded} phút) ${human} ${dayStrFormatted} (ONL)`);
        }
      }
    }

    if (notesForDetail.length) {
      changes.set(r0, { notes: notesForDetail, onlErrorCount });
    }
  }

  return { changes, problematicCells };
}

/**
 * Xử lý lỗi ONL từ master sheet (các cột "X-onl") - chỉ xử lý QUÊN CHECK IN/OUT
 * @param {Object} opts - Options { dryRun, testRows }
 * @return {Object} Kết quả xử lý
 */
function applyOnlAttendanceMissingOnly(opts) {
  opts = opts || {};
  const dryRun = typeof opts.dryRun === 'boolean' ? opts.dryRun : false;
  const testRows = (typeof opts.testRows === 'number' && opts.testRows > 0) ? Math.trunc(opts.testRows) : null;

  // ====== CONFIG ======
  const MASTER_FILE_ID = '1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4';
  const MASTER_SHEET_NAME = 'Chấm công th12/2025';
  const cfg = { morningStart: '08:30', afternoonStart: '13:15', cutoff: '12:00', lateThreshold: 30, maxTimesThreshold: 4 };

  Logger.log('1) Loading master sheet info...');
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  masterInfo.masterSh = masterSh;
  const month = parseMonthFromSheetName_(MASTER_SHEET_NAME) || 12;
  const headerMap = findHeaderCols_(masterInfo.header);

  Logger.log('2) Analyzing ONL attendance from sheet (missing only, chỉ theo data ô, không đăng ký ca)...');
  const result = prepareOnlAttendanceChangesFromSheet_(masterInfo, cfg, month, 'missing');
  const changes = result.changes || new Map();
  const problematicCells = result.problematicCells || [];

  Logger.log(`   Computed changes=${changes.size} problematic=${problematicCells.length}`);

  // Prepare arrays for writing
  const lastEmpRow = masterInfo.lastEmpRow;
  const noteCol = headerMap.noteCol || headerMap.detail3Col || headerMap.detail2Col; // Cột W
  const onlForgotCol = headerMap.onlForgotCol || null; // Cột V
  const noteArr = noteCol ? masterSh.getRange(1, noteCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];
  const onlForgotArr = onlForgotCol ? masterSh.getRange(1, onlForgotCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];

  const newNote = noteArr.slice();
  const newOnlForgot = onlForgotArr.slice();

  // Apply changes into arrays
  // LƯU Ý: r0 trong changes là index trong onlDataBlock (bắt đầu từ hàng 2)
  // - onlDataBlock[0] = hàng 2 trong sheet -> r0 = 0
  // - onlDataBlock[1] = hàng 3 trong sheet -> r0 = 1
  // - onlDataBlock[2] = hàng 4 trong sheet -> r0 = 2
  // Nhưng noteArr bắt đầu từ hàng 1:
  // - noteArr[0] = hàng 1 (header)
  // - noteArr[1] = hàng 2 -> tương ứng với r0 = 0
  // - noteArr[2] = hàng 3 -> tương ứng với r0 = 1
  // - noteArr[3] = hàng 4 -> tương ứng với r0 = 2
  // Vậy để map r0 sang index trong noteArr: noteIndex = r0 + 1
  for (const [r0, v] of changes.entries()) {
    const allNotes = v.notes || [];
    const onlErrorCount = v.onlErrorCount || 0;
    
    // Chuyển đổi r0 (index trong onlDataBlock) sang index trong noteArr
    const noteIndex = r0 + 1; // r0=0 -> noteIndex=1 (hàng 2), r0=1 -> noteIndex=2 (hàng 3), r0=2 -> noteIndex=3 (hàng 4)
    
    if (noteIndex < 0 || noteIndex >= newNote.length) {
      Logger.log(`Warning: noteIndex ${noteIndex} (from r0=${r0}) out of range [0, ${newNote.length - 1}]`);
      continue;
    }

    // Ghi note vào cột W (nối chuỗi) - chỉ xử lý quên check in/out
    if (noteCol && allNotes.length) {
      const prev = String(newNote[noteIndex] || '').trim();
      newNote[noteIndex] = (prev ? prev + '\n' : '') + allNotes.join('\n');
    }

    // Cộng số lỗi vào cột V
    if (onlForgotCol) {
      const prev = Number(newOnlForgot[noteIndex] || 0);
      newOnlForgot[noteIndex] = prev + onlErrorCount;
    }
  }

  // Preview
  if (dryRun) {
    Logger.log('applyOnlAttendanceMissingOnly (dryRun) preview sample:');
    let i = 0;
    for (const [r0, v] of changes.entries()) {
      const noteIndex = r0 + 1; // r0 là index trong onlDataBlock, noteIndex là index trong noteArr
      const actualRow = noteIndex + 1; // noteIndex=1 -> hàng 2, noteIndex=2 -> hàng 3, ...
      Logger.log(`PREVIEW Row ${actualRow} (r0=${r0}, noteIndex=${noteIndex}): ${v.notes.join('; ')} (count=${v.onlErrorCount})`);
      if (++i >= 50) break;
    }
    return { changesCount: changes.size, problematicCellsCount: problematicCells.length };
  }

  // If testRows specified -> write per-row for first N changes (safe)
  const changeEntries = Array.from(changes.entries());
  if (testRows) {
    Logger.log(`applyOnlAttendanceMissingOnly: testRows write mode N=${testRows} (per-row writes)`);
    const slice = changeEntries.slice(0, testRows);
    slice.forEach(([r0, v]) => {
      const noteIndex = r0 + 1; // r0 là index trong onlDataBlock, noteIndex là index trong noteArr
      const rowNum = noteIndex + 1; // noteIndex=1 -> hàng 2, noteIndex=2 -> hàng 3, ...
      if (noteCol) {
        const prev = String(noteArr[noteIndex] || '').trim();
        masterSh.getRange(rowNum, noteCol).setValue((prev ? prev + '\n' : '') + v.notes.join('\n'));
      }
      if (onlForgotCol) {
        masterSh.getRange(rowNum, onlForgotCol).setValue(Number(onlForgotArr[noteIndex] || 0) + Number(v.onlErrorCount || 0));
      }
      Logger.log(`WROTE row ${rowNum} (r0=${r0}, noteIndex=${noteIndex})`);
    });
    return { changesCount: changeEntries.length, written: slice.length };
  }

  // Otherwise full commit: write columns back
  const writes = [];
  if (noteCol) writes.push({ range: masterSh.getRange(1, noteCol, lastEmpRow, 1), values: newNote.map(x => [x || '']) });
  if (onlForgotCol) writes.push({ range: masterSh.getRange(1, onlForgotCol, lastEmpRow, 1), values: newOnlForgot.map(x => [x || 0]) });

  writes.forEach(w => w.range.setValues(w.values));
  Logger.log(`applyOnlAttendanceMissingOnly: full commit wrote ${writes.length} ranges`);
  return { changesCount: changes.size, written: writes.length };
}

/**
 * Xử lý lỗi TRỄ ONL từ master sheet (các cột "X-onl") - chỉ xử lý TRỄ
 * Trễ <= 30 phút: ghi vào cột S với (ONL), đếm ở cột R
 * Trễ > 30 phút: quy thành quên check in, KHÔNG ghi vào cột S
 * @param {Object} opts - Options { dryRun, testRows }
 * @return {Object} Kết quả xử lý
 */

/**
 * Xử lý lỗi TRỄ ONL từ master sheet (các cột "X-onl") - chỉ xử lý TRỄ
 * Trễ <= 30 phút: ghi vào cột S với (ONL), đếm ở cột R
 * Trễ > 30 phút: quy thành quên check in, KHÔNG ghi vào cột S (đã xử lý ở hàm missing)
 * @param {Object} opts - Options { dryRun, testRows }
 * @return {Object} Kết quả xử lý
 */
function applyOnlAttendanceLateOnly(opts) {
  opts = opts || {};
  const dryRun = typeof opts.dryRun === 'boolean' ? opts.dryRun : false;
  const testRows = (typeof opts.testRows === 'number' && opts.testRows > 0) ? Math.trunc(opts.testRows) : null;

  // ====== CONFIG ======
  const MASTER_FILE_ID = '1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4';
  const MASTER_SHEET_NAME = 'Chấm công th12/2025';
  const cfg = { morningStart: '08:30', afternoonStart: '13:15', cutoff: '12:00', lateThreshold: 30, maxTimesThreshold: 4 };

  Logger.log('1) Loading master sheet info...');
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  masterInfo.masterSh = masterSh;
  const month = parseMonthFromSheetName_(MASTER_SHEET_NAME) || 12;
  const headerMap = findHeaderCols_(masterInfo.header);

  Logger.log('2) Analyzing ONL attendance from sheet (late only, chỉ theo data ô, không đăng ký ca)...');
  const result = prepareOnlAttendanceChangesFromSheet_(masterInfo, cfg, month, 'late');
  const changes = result.changes || new Map();
  const problematicCells = result.problematicCells || [];

  Logger.log(`   Computed changes=${changes.size} problematic=${problematicCells.length}`);

  // Prepare arrays for writing
  const lastEmpRow = masterInfo.lastEmpRow;
  const lateNoteCol = headerMap.detail2Col || null; // Cột S (19) - ghi note TRỄ
  const totalLateCol = 18; // Cột R (18) - đếm số lượng trễ ONL
  const lateNoteArr = lateNoteCol ? masterSh.getRange(1, lateNoteCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];
  const totalLateArr = masterSh.getRange(1, totalLateCol, lastEmpRow, 1).getValues().map(r => r[0]);

  const newLateNote = lateNoteArr.slice();
  const newTotalLate = totalLateArr.slice();

  // Apply changes into arrays
  // LƯU Ý: r0 trong changes là index trong onlDataBlock (bắt đầu từ hàng 2)
  // - onlDataBlock[0] = hàng 2 trong sheet -> r0 = 0
  // - onlDataBlock[1] = hàng 3 trong sheet -> r0 = 1
  // - onlDataBlock[2] = hàng 4 trong sheet -> r0 = 2
  // Nhưng lateNoteArr bắt đầu từ hàng 1:
  // - lateNoteArr[0] = hàng 1 (header)
  // - lateNoteArr[1] = hàng 2 -> tương ứng với r0 = 0
  // - lateNoteArr[2] = hàng 3 -> tương ứng với r0 = 1
  // - lateNoteArr[3] = hàng 4 -> tương ứng với r0 = 2
  // Vậy để map r0 sang index trong lateNoteArr: noteIndex = r0 + 1
  for (const [r0, v] of changes.entries()) {
    const allNotes = v.notes || [];
    
    // Chỉ lấy các note TRỄ (trễ <= 30 phút), bỏ qua note quên check in/out
    const lateNotes = allNotes.filter(n => {
      if (typeof n !== 'string') return false;
      const nLower = n.toLowerCase();
      // Chỉ lấy note có "trễ" và không có "quên check"
      return nLower.includes('trễ') && !nLower.includes('quên check');
    });
    
    // Chuyển đổi r0 (index trong onlDataBlock) sang index trong lateNoteArr
    const noteIndex = r0 + 1; // r0=0 -> noteIndex=1 (hàng 2), r0=1 -> noteIndex=2 (hàng 3), r0=2 -> noteIndex=3 (hàng 4)
    
    if (noteIndex < 0 || noteIndex >= newLateNote.length) {
      Logger.log(`Warning: noteIndex ${noteIndex} (from r0=${r0}) out of range [0, ${newLateNote.length - 1}]`);
      continue;
    }

    // Ghi note TRỄ vào cột S (ghi đè, không append)
    if (lateNoteCol && lateNotes.length > 0) {
      newLateNote[noteIndex] = lateNotes.join('\n');
    } else if (lateNoteCol) {
      // Nếu không có note trễ, xóa nội dung cũ (ghi đè bằng rỗng)
      newLateNote[noteIndex] = '';
    }

    // Đếm số lượng trễ vào cột R
    newTotalLate[noteIndex] = lateNotes.length;
  }

  // Preview
  if (dryRun) {
    Logger.log('applyOnlAttendanceLateOnly (dryRun) preview sample:');
    let i = 0;
    for (const [r0, v] of changes.entries()) {
      const noteIndex = r0 + 1;
      const actualRow = noteIndex + 1;
      const lateNotes = (v.notes || []).filter(n => {
        if (typeof n !== 'string') return false;
        const nLower = n.toLowerCase();
        return nLower.includes('trễ') && !nLower.includes('quên check');
      });
      Logger.log(`PREVIEW Row ${actualRow} (r0=${r0}, noteIndex=${noteIndex}): ${lateNotes.join('; ')} (count=${lateNotes.length})`);
      if (++i >= 50) break;
    }
    return { changesCount: changes.size, problematicCellsCount: problematicCells.length };
  }

  // If testRows specified -> write per-row for first N changes (safe)
  const changeEntries = Array.from(changes.entries());
  if (testRows) {
    Logger.log(`applyOnlAttendanceLateOnly: testRows write mode N=${testRows} (per-row writes)`);
    const slice = changeEntries.slice(0, testRows);
    slice.forEach(([r0, v]) => {
      const noteIndex = r0 + 1;
      const rowNum = noteIndex + 1;
      const lateNotes = (v.notes || []).filter(n => {
        if (typeof n !== 'string') return false;
        const nLower = n.toLowerCase();
        return nLower.includes('trễ') && !nLower.includes('quên check');
      });
      
      if (lateNoteCol) {
        masterSh.getRange(rowNum, lateNoteCol).setValue(lateNotes.length > 0 ? lateNotes.join('\n') : '');
      }
      masterSh.getRange(rowNum, totalLateCol).setValue(lateNotes.length);
      Logger.log(`WROTE row ${rowNum} (r0=${r0}, noteIndex=${noteIndex}): ${lateNotes.length} late notes`);
    });
    return { changesCount: changeEntries.length, written: slice.length };
  }

  // Otherwise full commit: write columns back
  const writes = [];
  if (lateNoteCol) writes.push({ range: masterSh.getRange(1, lateNoteCol, lastEmpRow, 1), values: newLateNote.map(x => [x || '']) });
  writes.push({ range: masterSh.getRange(1, totalLateCol, lastEmpRow, 1), values: newTotalLate.map(x => [x || 0]) });

  writes.forEach(w => w.range.setValues(w.values));
  Logger.log(`applyOnlAttendanceLateOnly: full commit wrote ${writes.length} ranges`);
  return { changesCount: changes.size, written: writes.length };
}

/**
 * Hàm debug để kiểm tra một ô cụ thể trong sheet và so sánh với form data
 * @param {string} empCode - Mã nhân viên (ví dụ: MH0172)
 * @param {string} dayStr - Ngày (ví dụ: "29")
 */
function debugOnlCell(empCode, dayStr) {
  const MASTER_FILE_ID = '1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4';
  const MASTER_SHEET_NAME = 'Chấm công th12/2025';
  const FORM_FILE_ID = '1_mmyOMrX8cOW3bEqt6HxE5B7A0wxH5ud_SVyZEMMDQE';
  const SCHEDULE_FILE_ID = '1oKFAsC-mhAtA_yzHk8TwC3k5cCYzdNKFTgYSxfbDsSo';
  const SCHEDULE_SHEETS = ['LỊCH LÀM T12/2025', 'PAGE LỄ TÂN - LỊCH LÀM 2025'];
  const MASTER_EMP_FILE_ID = '1_szrWl2X-6Kcp7lpdl4HmBo7uciLqDGO-VWq1uie3HY';
  const MASTER_EMP_SHEET = 'MÃ SỐ NHÂN VIÊN';
  const onlStartCol = 119; // DO

  // Chuẩn hóa dayStr
  const dayNum = parseInt(dayStr, 10);
  if (isNaN(dayNum) || dayNum < 1 || dayNum > 31) {
    Logger.log(`ERROR: Invalid dayStr "${dayStr}"`);
    return;
  }
  const normalizedDayStr = String(dayNum);

  Logger.log(`=== DEBUG ONL CELL ===`);
  Logger.log(`Employee: ${empCode}`);
  Logger.log(`Day: ${normalizedDayStr} (raw: "${dayStr}")`);

  const ss = SpreadsheetApp.openById(MASTER_FILE_ID);
  const masterSh = ss.getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) {
    Logger.log(`ERROR: Không tìm thấy sheet ${MASTER_SHEET_NAME}`);
    return;
  }

  // Tìm cột cho ngày (chuẩn hóa)
  const header = masterSh.getRange(1, onlStartCol, 1, 31).getValues()[0];
  let colIndex = null;
  const dayToCol = new Map();
  for (let c = 0; c < header.length; c++) {
    const headerVal = String(header[c] || '').trim();
    const match = headerVal.match(/^(\d{1,2})-onl$/i);
    if (match) {
      const hDayNum = parseInt(match[1], 10);
      if (hDayNum >= 1 && hDayNum <= 31) {
        const hDayStr = String(hDayNum);
        dayToCol.set(hDayStr, c);
        if (hDayStr === normalizedDayStr) {
          colIndex = c;
        }
      }
    }
  }

  if (colIndex === null) {
    Logger.log(`ERROR: Không tìm thấy cột cho ngày ${normalizedDayStr}-onl`);
    Logger.log(`  Available columns: ${Array.from(dayToCol.keys()).sort((a, b) => parseInt(a) - parseInt(b)).join(', ')}`);
    return;
  }

  // Tìm hàng cho nhân viên
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  const row1 = masterInfo.empToRow.get(empCode.toUpperCase());
  if (!row1) {
    Logger.log(`ERROR: Không tìm thấy nhân viên ${empCode} trong master sheet`);
    return;
  }

  const actualCol = onlStartCol + colIndex;
  const colLetter = columnNumberToLetter_(actualCol);
  const cell = masterSh.getRange(row1, actualCol);

  Logger.log(`\n=== SHEET CELL INFO ===`);
  Logger.log(`Cell: ${colLetter}${actualCol} (row ${row1}, col ${actualCol})`);
  Logger.log(`Value: "${cell.getValue()}"`);
  Logger.log(`Display Value: "${cell.getDisplayValue()}"`);
  Logger.log(`Formula: "${cell.getFormula()}"`);
  Logger.log(`Number Format: "${cell.getNumberFormat()}"`);

  // Kiểm tra form data
  Logger.log(`\n=== FORM DATA CHECK ===`);
  try {
    const formData = loadOnlFormData_(FORM_FILE_ID);
    const nameToEmpMap = buildNameToEmpMapForOnl_(formData, MASTER_EMP_FILE_ID, MASTER_EMP_SHEET);
    
    // Tìm entries cho empCode và dayStr
    const empEntries = formData.filter(entry => {
      const normalizedName = normalize_(entry.fullName);
      const mappedEmpCode = nameToEmpMap.get(normalizedName);
      return mappedEmpCode === empCode.toUpperCase() && entry.day === normalizedDayStr;
    });

    Logger.log(`Found ${empEntries.length} form entries for ${empCode} on day ${normalizedDayStr}:`);
    empEntries.forEach((entry, idx) => {
      Logger.log(`  Entry ${idx + 1}: ${entry.action} ${entry.shiftType} at ${Utilities.formatDate(entry.timestamp, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss')}`);
    });

    // Group form data
    const empDayData = new Map();
    formData.forEach(entry => {
      const normalizedName = normalize_(entry.fullName);
      const mappedEmpCode = nameToEmpMap.get(normalizedName);
      if (mappedEmpCode !== empCode.toUpperCase()) return;
      
      let dayStrNorm = entry.day;
      const dayNum2 = parseInt(entry.day, 10);
      if (!isNaN(dayNum2) && dayNum2 >= 1 && dayNum2 <= 31) {
        dayStrNorm = String(dayNum2);
      }

      if (dayStrNorm !== normalizedDayStr) return;

      if (!empDayData.has(mappedEmpCode)) {
        empDayData.set(mappedEmpCode, new Map());
      }
      const dayMap = empDayData.get(mappedEmpCode);
      if (!dayMap.has(dayStrNorm)) {
        dayMap.set(dayStrNorm, {
          morning: { in: null, out: null },
          afternoon: { in: null, out: null }
        });
      }
      const dayData = dayMap.get(dayStrNorm);
      const session = dayData[entry.shiftType];
      if (entry.action === 'in') {
        session.in = entry.timestamp;
      } else if (entry.action === 'out') {
        session.out = entry.timestamp;
      }
    });

    const dayData = empDayData.get(empCode.toUpperCase())?.get(normalizedDayStr);
    if (dayData) {
      Logger.log(`\nGrouped form data for ${empCode} day ${normalizedDayStr}:`);
      Logger.log(`  Morning: in=${dayData.morning.in ? Utilities.formatDate(dayData.morning.in, Session.getScriptTimeZone(), 'HH:mm') : 'null'}, out=${dayData.morning.out ? Utilities.formatDate(dayData.morning.out, Session.getScriptTimeZone(), 'HH:mm') : 'null'}`);
      Logger.log(`  Afternoon: in=${dayData.afternoon.in ? Utilities.formatDate(dayData.afternoon.in, Session.getScriptTimeZone(), 'HH:mm') : 'null'}, out=${dayData.afternoon.out ? Utilities.formatDate(dayData.afternoon.out, Session.getScriptTimeZone(), 'HH:mm') : 'null'}`);
    } else {
      Logger.log(`No grouped form data found for ${empCode} day ${normalizedDayStr}`);
    }
  } catch (e) {
    Logger.log(`Error checking form data: ${e.toString()}`);
  }

  // Kiểm tra schedule
  Logger.log(`\n=== SCHEDULE CHECK ===`);
  try {
    const formData = loadOnlFormData_(FORM_FILE_ID);
    const nameToEmpMap = buildNameToEmpMapForOnl_(formData, MASTER_EMP_FILE_ID, MASTER_EMP_SHEET);
    const scheduleMap = loadOnlScheduleRegistrations_(SCHEDULE_FILE_ID, SCHEDULE_SHEETS, nameToEmpMap);
    
    const empSchedule = scheduleMap.get(empCode.toUpperCase());
    if (empSchedule) {
      const daySessions = empSchedule.get(normalizedDayStr);
      if (daySessions) {
        Logger.log(`Schedule for ${empCode} day ${normalizedDayStr}: ${Array.from(daySessions).join(', ')}`);
      } else {
        Logger.log(`No schedule registration for ${empCode} day ${normalizedDayStr}`);
        Logger.log(`  Registered days: ${Array.from(empSchedule.keys()).sort((a, b) => parseInt(a) - parseInt(b)).join(', ')}`);
      }
    } else {
      Logger.log(`No schedule found for ${empCode}`);
    }
  } catch (e) {
    Logger.log(`Error checking schedule: ${e.toString()}`);
  }

  Logger.log(`\nURL: https://docs.google.com/spreadsheets/d/${MASTER_FILE_ID}/edit#gid=${masterSh.getSheetId()}&range=${colLetter}${row1}`);
}

// ==================== XỬ LÝ CHECK IN/OUT OFF TỪ GOOGLE FORM ====================

/**
 * Load dữ liệu check in/out OFF từ Google Form responses
 * @param {string} formFileId - File ID của Google Form responses sheet
 * @return {Array} Array of objects với format:
 *   {
 *     timestamp: Date,      // Thời gian check-in/out
 *     fullName: string,     // Họ tên
 *     checkType: string,     // "CA OFFLINE"
 *     workShift: string,     // "Check out ca chiều"
 *     date: "10/4",         // "DD/MM"
 *     day: "10",            // Ngày
 *     month: "4",           // Tháng
 *     shiftType: "afternoon", // "morning" hoặc "afternoon"
 *     action: "out"         // "in" hoặc "out"
 *   }
 */
function loadOffFormData_(formFileId) {
  const ss = SpreadsheetApp.openById(formFileId);
  const sheets = ss.getSheets();
  if (sheets.length === 0) throw new Error('Không tìm thấy sheet trong form file');

  const formSheet = sheets[0]; // Sheet đầu tiên chứa responses
  const lr = formSheet.getLastRow();
  const lc = formSheet.getLastColumn();

  if (lr < 2) {
    Logger.log('Form sheet không có dữ liệu (chỉ có header)');
    return [];
  }

  const values = formSheet.getRange(1, 1, lr, lc).getValues();
  const data = [];

  // Cột A (index 0): Timestamp
  // Cột C (index 2): Họ tên hoặc Mã nhân viên
  // Cột E (index 4): EM CHẤM CÔNG - chứa "CA ONLINE" hoặc "CA OFFLINE"
  // Cột G (index 6): EM CHẤM CÔNG CHO NGÀY NÀO - chứa Date object (ngày ca làm việc)
  // Cột H (index 7): CA LÀM VIỆC CỦA EM - "Check in ca sáng", "Check out ca chiều", ...
  // Quy tắc 24h: timestamp (A) phải cùng ngày với cột G. Check-out qua ngày hôm sau → bỏ qua, không ghi (giống ONL).

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const timestamp = row[0]; // Cột A
    const fullName = String(row[2] || '').trim(); // Cột C
    const checkType = String(row[4] || '').trim(); // Cột E
    const checkDate = row[6]; // Cột G - EM CHẤM CÔNG CHO NGÀY NÀO (Date object)
    const workShift = String(row[7] != null && row[7] !== '' ? row[7] : '').trim(); // Cột H - CA LÀM VIỆC CỦA EM

    // Chỉ lấy các entry có "CA OFFLINE"
    if (!checkType.toUpperCase().includes('OFFLINE')) continue;
    if (!fullName) continue;

    // Timestamp phải là Date
    if (!timestamp || !(timestamp instanceof Date) || isNaN(timestamp.getTime())) continue;

    // Parse ngày từ cột G (EM CHẤM CÔNG CHO NGÀY NÀO)
    let dateStr = null, dayStr = null, monthStr = null;
    if (checkDate instanceof Date && !isNaN(checkDate.getTime())) {
      const day = checkDate.getDate();
      const month = checkDate.getMonth() + 1;
      // Chuẩn hóa dayStr: đảm bảo là string "1", "2", ..., "31" (không có leading zero)
      dayStr = String(day);
      monthStr = String(month);
      dateStr = `${day}/${month}`;
    }
    if (!dateStr || !dayStr) continue;

    // Quy tắc 24h: timestamp phải cùng ngày với checkDate (cột G). Check-out qua ngày hôm sau → bỏ qua, không ghi.
    try {
      const tz = Session.getScriptTimeZone();
      const tsKey = Utilities.formatDate(timestamp, tz, 'yyyy-MM-dd');
      const cdKey = Utilities.formatDate(checkDate, tz, 'yyyy-MM-dd');
      if (tsKey !== cdKey) {
        Logger.log(`Skip OFF cross-day entry row=${r + 1}: ${fullName} ${checkType} "${workShift}" checkDate=${cdKey} ts=${tsKey}`);
        continue;
      }
    } catch (e) {
      Logger.log(`Skip OFF entry row=${r + 1} due to date-compare error: ${e && e.message ? e.message : e}`);
      continue;
    }

    // Parse ca và action từ workShift (cột H - CA LÀM VIỆC CỦA EM)
    const workShiftLower = workShift.toLowerCase();
    let shiftType = null;
    let action = null;

    if (workShiftLower.includes('sáng') || workShiftLower.includes('sang')) {
      shiftType = 'morning';
    } else if (workShiftLower.includes('chiều') || workShiftLower.includes('chieu')) {
      shiftType = 'afternoon';
    }

    if (workShiftLower.includes('check in') || workShiftLower.includes('checkin')) {
      action = 'in';
    } else if (workShiftLower.includes('check out') || workShiftLower.includes('checkout')) {
      action = 'out';
    }

    if (!shiftType || !action) continue;

    data.push({
      timestamp: timestamp,
      fullName: fullName,
      checkType: checkType,
      workShift: workShift,
      date: dateStr,
      day: dayStr,
      month: monthStr,
      shiftType: shiftType,
      action: action
    });
  }

  Logger.log(`Loaded ${data.length} OFF check-in/out entries from form`);
  return data;
}

/**
 * Load đăng ký ca OFF từ sheet đăng ký ca làm
 * @param {string} scheduleFileId - File ID của sheet đăng ký ca làm
 * @param {Array<string>} sheetNames - Tên các sheet cần đọc
 * @param {Map} nameToEmpMap - Map từ tên (normalized) sang mã nhân viên
 * @return {Map} Map<empCode, Map<dayStr, Set<sessionName>>>
 *   Ví dụ: Map("MH0010", Map("7", Set("morning", "afternoon")))
 */
function loadOffScheduleRegistrations_(scheduleFileId, sheetNames, nameToEmpMap) {
  const ss = SpreadsheetApp.openById(scheduleFileId);
  const scheduleMap = new Map();
  const unmatched = new Set();

  // Helper để map shift label sang session
  const shiftToSession = (s) => {
    if (!s) return null;
    const t = String(s || '').toUpperCase();
    if (t.startsWith('ST')) return 'morning';
    if (t.startsWith('CT')) return 'afternoon';
    if (t.startsWith('TT') || t.startsWith('TTT') || t.startsWith('T')) return 'evening';
    if (t.includes('SANG')) return 'morning';
    if (t.includes('CHIEU')) return 'afternoon';
    if (t.includes('TOI') || t.includes('TỐI')) return 'evening';
    return 'afternoon';
  };

  sheetNames.forEach(sheetName => {
    const sh = ss.getSheetByName(sheetName);
    if (!sh) {
      Logger.log('Warning: sheet not found ' + sheetName);
      return;
    }

    const lr = sh.getLastRow();
    const lc = sh.getLastColumn();
    const vals = sh.getRange(1, 1, lr, lc).getValues();

    // Tìm header row với date cells và shift row
    let dateRow = null, shiftRow = null, nameCol = null, empCodeCol = null, nameHeaderRow = null;

    for (let r = 0; r < Math.min(10, vals.length); r++) {
      let dateCount = 0;
      for (let c = 0; c < Math.min(50, vals[r].length); c++) {
        const cell = vals[r][c];
        if (cell instanceof Date || /^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(String(cell || ''))) dateCount++;
      }
      if (dateCount >= 1 && dateRow === null) dateRow = r;
    }

    if (dateRow !== null && dateRow + 1 < vals.length) shiftRow = dateRow + 1;

    // Tìm cột mã nhân viên hoặc tên
    for (let r = 0; r <= Math.min(10, vals.length - 1); r++) {
      for (let c = 0; c < Math.min(10, vals[r].length); c++) {
        const h = normalize_(vals[r][c] || '');
        if (!empCodeCol && (h.includes('ma') || h.includes('ma so') || h.includes('mã') || h.includes('mã số') || h.includes('ma nhan vien') || h.includes('mã nhân viên') || h.includes('mã nv'))) {
          empCodeCol = c;
          nameHeaderRow = r;
          break;
        }
        if (!nameCol && (h.includes('cvts') || h.includes('ho ten') || h.includes('họ tên') || h.includes('tên'))) {
          nameCol = c;
          nameHeaderRow = r;
          break;
        }
      }
      if (empCodeCol !== null || nameCol !== null) break;
    }

    if (empCodeCol === null && nameCol === null) {
      Logger.log('Could not find name/code column in ' + sheetName + ' - falling back to column B');
      nameCol = 1;
      nameHeaderRow = 0;
    }

    // Xác định các cột ngày
    const dateCols = []; // array of {colIndex, dayStr, shiftLabel}
    if (dateRow !== null) {
      for (let c = 0; c < vals[dateRow].length; c++) {
        const v = vals[dateRow][c];
        let day = parseDayFromValue_(v);
        // Hỗ trợ format dd/mm/yyyy
        if (!day && /^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(String(v || '').trim())) {
          day = String(Number(String(v).split('/')[0]));
          if (!/^[1-9]$|^[12]\d$|^3[01]$/.test(day)) day = null;
        }
        if (day) {
          const shiftLabel = (shiftRow !== null && vals[shiftRow] && vals[shiftRow][c]) ? String(vals[shiftRow][c] || '') : '';
          dateCols.push({ col: c, dayStr: day, shiftLabel });
        }
      }
    }

    // Duyệt các dòng nhân viên
    const startRow = (nameHeaderRow !== null) ? nameHeaderRow + 1 : 2;
    for (let r = startRow; r < lr; r++) {
      let rawCode = (typeof empCodeCol === 'number') ? String(vals[r][empCodeCol] || '').trim().toUpperCase() : '';
      let empCode = '';

      if (rawCode && /^MH\d{4}$/i.test(rawCode)) {
        empCode = rawCode;
      } else {
        // Fallback to name-based mapping
        const rawName = nameCol !== null ? String(vals[r][nameCol] || '').trim() : '';
        if (!rawName) continue;
        const n = normalize_(rawName.replace(/\(.*\)$/, '').trim());
        empCode = nameToEmpMap.get(n) || '';
        if (!empCode && rawCode) {
          const m = String(rawCode).match(/(MH\d{4})/i);
          if (m) empCode = m[1].toUpperCase();
        }
        if (!empCode) {
          unmatched.add(rawName || rawCode);
          continue;
        }
      }

      // Tìm các ca OFF
      const empSched = new Map();
      let hasOffRegistration = false;

      for (const dc of dateCols) {
        const c = dc.col;
        const cellVal = String(vals[r][c] || '').trim().toUpperCase();
        // Chỉ lấy các ô có giá trị chính xác là "OFF"
        if (cellVal === 'OFF' || cellVal === 'OFF ▼' || cellVal.startsWith('OFF')) {
          hasOffRegistration = true;
          const sess = shiftToSession(dc.shiftLabel || '');
          if (!empSched.has(dc.dayStr)) empSched.set(dc.dayStr, new Set());
          empSched.get(dc.dayStr).add(sess);
        }
      }

      if (hasOffRegistration) {
        scheduleMap.set(empCode, empSched);
      }
    }
  });

  if (unmatched.size) Logger.log('OFF Schedule load: unmatched names count=' + unmatched.size + ' sample=' + Array.from(unmatched).slice(0, 5).join(', '));
  Logger.log(`Loaded ${scheduleMap.size} employees with OFF registrations`);
  return scheduleMap;
}

/**
 * Build name-to-employee-code map từ form data và master employees file (cho OFF)
 * @param {Array} formData - Dữ liệu từ Google Form
 * @param {string} masterEmpFileId - File ID của master employees
 * @param {string} masterEmpSheet - Tên sheet chứa danh sách nhân viên
 * @return {Map} Map<normalizedName, empCode>
 */
function buildNameToEmpMapForOff_(formData, masterEmpFileId, masterEmpSheet) {
  // Lấy tất cả tên unique từ formData
  const uniqueNames = new Set();
  formData.forEach(entry => {
    if (entry.fullName) uniqueNames.add(entry.fullName);
  });

  // Load map từ master employees file
  const nameMap = buildNameToEmpMap_(masterEmpFileId, masterEmpSheet);

  // Tạo map từ tên trong form sang mã nhân viên
  const formToEmpMap = new Map();
  uniqueNames.forEach(name => {
    const normalized = normalize_(name);
    const empCode = nameMap.get(normalized);
    if (empCode) {
      formToEmpMap.set(normalized, empCode);
    } else {
      // Thử fuzzy match
      const tokens = normalized.split(' ');
      for (const [mn, code] of nameMap.entries()) {
        let ok = true;
        for (const t of tokens) {
          if (t && !mn.includes(t)) {
            ok = false;
            break;
          }
        }
        if (ok) {
          formToEmpMap.set(normalized, code);
          break;
        }
      }
    }
  });

  Logger.log(`Built name to emp map for OFF: ${formToEmpMap.size} mappings from ${uniqueNames.size} unique names`);
  return formToEmpMap;
}

/**
 * Parse nội dung ô OFF: trích times (HH:mm) và loại ca từ chuỗi như "08:32\n12:25\n16:50\noff 2 ca"
 * @param {*} cell - Giá trị ô (string/Date/number)
 * @return {{ times: string[], shiftType: 'morning'|'afternoon'|'two' } | null} - null nếu không parse được
 */
function parseOffCell_(cell) {
  const times = extractTimesFromCell_(cell);
  if (!times || times.length === 0) return null;

  const text = String(cell || '').trim().toLowerCase();
  // Xác định loại ca từ phần không phải HH:mm (vd: "off 2 ca", "off ca sáng", "off ca chiều")
  if (/off\s*2\s*ca|2\s*ca|off\s*2ca/.test(text)) return { times, shiftType: 'two' };
  if (/sáng|sang/.test(text) && !/chiều|chieu/.test(text)) return { times, shiftType: 'morning' };
  if (/chiều|chieu/.test(text)) return { times, shiftType: 'afternoon' };
  // Mặc định: 4 mốc => 2 ca, 2 mốc => phân biệt theo giờ (trước 12h = sáng, sau = chiều)
  const cutoff = timeStrToMinutes_('12:00') || 720;
  if (times.length >= 4) return { times, shiftType: 'two' };
  if (times.length === 2) {
    const m = timeStrToMinutes_(times[0]);
    return { times, shiftType: (m !== null && m < cutoff) ? 'morning' : 'afternoon' };
  }
  if (times.length === 3) return { times, shiftType: 'two' }; // 3 mốc = 2 ca thiếu 1 (vd: quên check in chiều)
  return { times, shiftType: 'morning' };
}

/**
 * Từ (times, shiftType) và giờ bắt đầu ca, suy ra morning/afternoon in, out, lateMinutes
 * - "off 2 ca" + 4 times: [in_sáng, out_sáng, in_chiều, out_chiều]
 * - "off 2 ca" + 3 times: [in_sáng, out_sáng, out_chiều] → thiếu in_chiều
 * - "off ca sáng" + 2 times: [in, out]
 * - "off ca chiều" + 2 times: [in, out]
 */
function buildOffSessionsFromParsed_(parsed, morningStartMin, afternoonStartMin) {
  if (!parsed || !parsed.times || parsed.times.length === 0) return [];
  const t = parsed.times.slice().sort((a, b) => (timeStrToMinutes_(a) || 0) - (timeStrToMinutes_(b) || 0));
  const out = [];

  const late = (inStr, startMin) => {
    const m = timeStrToMinutes_(inStr);
    if (m === null || startMin === null) return 0;
    return Math.max(0, m - startMin);
  };

  if (parsed.shiftType === 'morning') {
    const inStr = t[0];
    const outStr = t.length >= 2 ? t[1] : null;
    out.push({
      name: 'morning',
      in: inStr,
      out: outStr,
      lateMinutes: late(inStr, morningStartMin)
    });
  } else if (parsed.shiftType === 'afternoon') {
    const inStr = t[0];
    const outStr = t.length >= 2 ? t[1] : null;
    out.push({
      name: 'afternoon',
      in: inStr,
      out: outStr,
      lateMinutes: late(inStr, afternoonStartMin)
    });
  } else if (parsed.shiftType === 'two') {
    if (parsed.times.length >= 4) {
      // Thứ tự TRONG Ô = [in_sáng, out_sáng, in_chiều, out_chiều] (giống ONL: không sort để tránh gán sai khi out sáng muộn)
      const order = parsed.times.slice(0, 4);
      out.push({
        name: 'morning',
        in: order[0],
        out: order[1],
        lateMinutes: late(order[0], morningStartMin)
      });
      out.push({
        name: 'afternoon',
        in: order[2],
        out: order[3],
        lateMinutes: late(order[2], afternoonStartMin)
      });
    } else if (t.length === 3) {
      // Phân tích dựa trên cutoff (12:00) và thời gian bắt đầu ca chiều (thường 13:15)
      const cutoff = timeStrToMinutes_('12:00') || 720;
      const afternoonStart = afternoonStartMin || timeStrToMinutes_('13:15') || 795;
      const afternoonStartThreshold = afternoonStart - 30; // 12:45 - ngưỡng để phân biệt out sáng muộn vs in chiều
      const t0 = timeStrToMinutes_(t[0]);
      const t1 = timeStrToMinutes_(t[1]);
      const t2 = timeStrToMinutes_(t[2]);
      
      if (t0 !== null && t1 !== null && t2 !== null) {
        // Với 3 times và "off 2 ca", có 3 trường hợp:
        // 1. [in_sáng, out_sáng, in_chiều] - thiếu out_chiều (t1 < afternoonStartThreshold và t2 gần afternoonStart)
        // 2. [in_sáng, out_sáng, out_chiều] - thiếu in_chiều (t1 < afternoonStartThreshold và t2 xa afternoonStart)
        // 3. [in_sáng, in_chiều, out_chiều] - thiếu out_sáng (t1 >= afternoonStartThreshold)
        
        if (t1 < afternoonStartThreshold) {
          // t1 là out sáng (trước 12:45, có thể muộn nhưng vẫn là out sáng)
          // Kiểm tra t2: nếu t2 gần thời gian bắt đầu ca chiều (13:15) thì là in_chiều, ngược lại là out_chiều
          const diffToAfternoonStart = Math.abs(t2 - afternoonStart);
          const threshold = 60; // 1 giờ - nếu t2 cách afternoonStart < 1 giờ thì coi là in_chiều
          
          if (t2 >= cutoff && diffToAfternoonStart < threshold) {
            // t2 gần thời gian bắt đầu ca chiều (13:15) → t2 là in_chiều
            // [in_sáng, out_sáng, in_chiều] - thiếu out_chiều
            out.push({
              name: 'morning',
              in: t[0],
              out: t[1],
              lateMinutes: late(t[0], morningStartMin)
            });
            out.push({
              name: 'afternoon',
              in: t[2],
              out: null,
              lateMinutes: late(t[2], afternoonStartMin)
            });
          } else {
            // t2 xa thời gian bắt đầu ca chiều → t2 là out_chiều
            // [in_sáng, out_sáng, out_chiều] - thiếu in_chiều
            out.push({
              name: 'morning',
              in: t[0],
              out: t[1],
              lateMinutes: late(t[0], morningStartMin)
            });
            out.push({
              name: 'afternoon',
              in: null,
              out: t[2],
              lateMinutes: 0
            });
          }
        } else {
          // t1 >= afternoonStartThreshold (>= 12:45) → t1 là in_chiều
          // [in_sáng, in_chiều, out_chiều] - thiếu out_sáng
          out.push({
            name: 'morning',
            in: t[0],
            out: null,
            lateMinutes: late(t[0], morningStartMin)
          });
          out.push({
            name: 'afternoon',
            in: t[1],
            out: t[2],
            lateMinutes: late(t[1], afternoonStartMin)
          });
        }
      }
    } else if (t.length === 2) {
      // Phân tích dựa trên cutoff
      const cutoff = timeStrToMinutes_('12:00') || 720;
      const t0 = timeStrToMinutes_(t[0]);
      const t1 = timeStrToMinutes_(t[1]);
      
      if (t0 !== null && t1 !== null) {
        if (t1 < cutoff) {
          // Cả 2 đều trước 12h -> ca sáng
          out.push({
            name: 'morning',
            in: t[0],
            out: t[1],
            lateMinutes: late(t[0], morningStartMin)
          });
        } else if (t0 >= cutoff) {
          // Cả 2 đều sau 12h -> ca chiều
          out.push({
            name: 'afternoon',
            in: t[0],
            out: t[1],
            lateMinutes: late(t[0], afternoonStartMin)
          });
        } else {
          // t0 < cutoff < t1 -> 2 ca nhưng thiếu out_sáng và in_chiều
          out.push({
            name: 'morning',
            in: t[0],
            out: null,
            lateMinutes: late(t[0], morningStartMin)
          });
          out.push({
            name: 'afternoon',
            in: null,
            out: t[1],
            lateMinutes: 0
          });
        }
      }
    }
  }

  return out;
}

/**
 * Phân tích lỗi OFF chỉ từ dữ liệu trong các cột CB-DF, không đối chiếu đăng ký ca.
 * Duyệt mọi nhân viên, mọi cột CB-DF; ô nào có nội dung thì parse và phát hiện quên check in/out + trễ.
 * @param {Object} masterInfo - Thông tin master sheet
 * @param {Object} cfg - Config (morningStart, afternoonStart, lateThreshold)
 * @param {number} month - Tháng
 * @param {string} mode - 'both' | 'late' | 'missing'
 * @return {Object} { changes: Map<r0, {notes, offErrorCount}>, problematicCells: Array }
 */
function prepareOffAttendanceChangesFromSheet_(masterInfo, cfg, month, mode) {
  const changes = new Map();
  const problematicCells = [];
  const runLate = (mode === 'both' || mode === 'late');
  const runMissing = (mode === 'both' || mode === 'missing');

  const humanForSession = (s) => (s === 'morning' ? 'ca sáng' : s === 'afternoon' ? 'ca chiều' : 'ca');

  const offStartCol = 80; // CB
  const offEndCol = 110; // DF
  const offColCount = offEndCol - offStartCol + 1;
  const header = masterInfo.header || [];
  const offDayToCol = new Map();

  // Tìm các cột ngày trong range CB-DF (ngày 1 đến 31)
  // Logic: CB (80) = ngày 1, CC (81) = ngày 2, ..., DF (110) = ngày 31
  // Tìm theo header trước, nếu không tìm thấy thì dùng vị trí cố định
  for (let c = offStartCol - 1; c < Math.min(offStartCol - 1 + offColCount, header.length); c++) {
    const h = String(header[c] || '').trim();
    // Các cột CB-DF có thể có header là số ngày (1, 2, ..., 31) hoặc "1-off", "2-off", etc.
    const dayMatch = h.match(/^(\d{1,2})(?:-off)?$/i);
    if (dayMatch) {
      const dayNum = parseInt(dayMatch[1], 10);
      if (dayNum >= 1 && dayNum <= 31) {
        offDayToCol.set(String(dayNum), c + 1);
      }
    }
  }
  
  // Nếu không tìm thấy cột nào theo header, dùng vị trí cố định: CB (80) = ngày 1, CC (81) = ngày 2, ...
  if (offDayToCol.size === 0) {
    Logger.log('Không tìm thấy cột OFF day columns theo header, sử dụng vị trí cố định: CB (80) = ngày 1, CC (81) = ngày 2, ...');
    for (let dayNum = 1; dayNum <= 31; dayNum++) {
      const colIndex = offStartCol - 1 + (dayNum - 1); // dayNum 1 -> index 79 (CB), dayNum 2 -> index 80 (CC), ...
      if (colIndex < header.length) {
        offDayToCol.set(String(dayNum), colIndex + 1);
      }
    }
  }

  Logger.log(`Found ${offDayToCol.size} OFF day columns (CB-DF) — không dùng đăng ký ca, chỉ từ data ô`);

  const lastEmpRow = masterInfo.lastEmpRow;
  if (offDayToCol.size === 0 || !masterInfo.masterSh || lastEmpRow < 2) {
    return { changes, problematicCells };
  }

  const dataRowCount = lastEmpRow - 1;
  const offDataBlock = masterInfo.masterSh.getRange(2, offStartCol, dataRowCount, offColCount).getValues();

  // Duyệt theo hàng (mỗi hàng = một nhân viên theo thứ tự trong sheet)
  const empRows = []; // [ { r0, empCode } ]
  for (const [empCode, r1] of masterInfo.empToRow.entries()) {
    // Bỏ qua hàng 1 (header)
    if (r1 === 1) continue;
    const r0 = r1 - 2; // Hàng 2 -> index 0, hàng 3 -> index 1, hàng 4 -> index 2, ...
    if (r0 < 0 || r0 >= offDataBlock.length) continue;
    empRows.push({ r0, empCode });
  }

  for (const { r0, empCode } of empRows) {
    let notesForDetail = [];
    let offErrorCount = 0;
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(empCode) : undefined;
    const scheduleTemplate = getEmployeeSchedule_(empCode, cfg, role);
    
    // Kiểm tra và fallback về default nếu schedule không có morning/afternoon
    const morningStart = scheduleTemplate.morning?.start || cfg.morningStart || SPECIAL_SCHEDULES.default.morningStart;
    const afternoonStart = scheduleTemplate.afternoon?.start || cfg.afternoonStart || SPECIAL_SCHEDULES.default.afternoonStart;
    const morningStartMin = timeStrToMinutes_(morningStart);
    const afternoonStartMin = timeStrToMinutes_(afternoonStart);

    for (const [dayStr, col1] of offDayToCol.entries()) {
      const c0 = col1 - offStartCol;
      if (c0 < 0 || c0 >= offColCount) continue;

      const cellValue = offDataBlock[r0][c0];
      
      // Kiểm tra chặt chẽ: chỉ xử lý khi ô có dữ liệu thực sự
      if (cellValue === null || cellValue === undefined || cellValue === '') continue;
      
      if (typeof cellValue === 'number') {
        if (cellValue === 0 || isNaN(cellValue) || !isFinite(cellValue)) continue;
      }
      
      const cellValueStr = String(cellValue || '').trim();
      if (!cellValueStr) continue;
      
      // Parse và kiểm tra kỹ: phải có times thực sự
      const parsed = parseOffCell_(cellValue);
      if (!parsed || !parsed.times || !Array.isArray(parsed.times) || parsed.times.length === 0) continue;
      
      // Kiểm tra thêm: các times phải là string hợp lệ (HH:mm format)
      const validTimes = parsed.times.filter(t => {
        if (typeof t !== 'string') return false;
        const timeMatch = t.match(/^([01]?\d|2[0-3]):[0-5]\d$/);
        return timeMatch !== null;
      });
      if (validTimes.length === 0) continue;
      
      parsed.times = validTimes;

      const sessions = buildOffSessionsFromParsed_(parsed, morningStartMin, afternoonStartMin);
      if (!sessions || sessions.length === 0) continue;
      
      const dayStrFormatted = `${parseInt(dayStr, 10)}/${month}`;

      for (const session of sessions) {
        // Chỉ xử lý session có dữ liệu thực sự (có ít nhất in hoặc out)
        if (!session || (session.in === null && session.out === null)) continue;
        
        const human = humanForSession(session.name);

        if (runMissing) {
          // Chỉ xử lý quên check in/out
          if (session.in == null && session.out != null) {
            notesForDetail.push(`- Quên check in ${human} ${dayStrFormatted} (OFF ngoài)`);
            offErrorCount++;
          } else if (session.in != null && session.out == null) {
            notesForDetail.push(`- Quên check out ${human} ${dayStrFormatted} (OFF ngoài)`);
            offErrorCount++;
          } else if (session.in && (typeof session.lateMinutes === 'number' && session.lateMinutes > 0)) {
            // Trễ >= 30 phút: quy thành quên check in (chỉ xử lý trong mode missing). Đồng bộ với highlight: không bắt buộc có session.out.
            const lateMin = Math.round(session.lateMinutes);
            const threshold = cfg.lateThreshold || 30;
            if (lateMin >= threshold) {
              notesForDetail.push(`- Check in trễ từ 30 phút trở lên (${lateMin} phút) ${human} ${dayStrFormatted} (OFF ngoài)`);
              offErrorCount++;
            }
          }
        }

        if (runLate && session.in && (typeof session.lateMinutes === 'number' && session.lateMinutes > 0)) {
          // Chỉ xử lý trễ < 30 phút (trễ >= 30 đã được xử lý ở mode missing). Đồng bộ với highlight: không bắt buộc có session.out.
          const lateMin = Math.round(session.lateMinutes);
          const threshold = cfg.lateThreshold || 30;
          
          // Trễ < 30 phút là lỗi TRỄ, ghi cột S
          if (lateMin > 0 && lateMin < threshold) {
            notesForDetail.push(`- Check in trễ dưới 30 phút (${lateMin} phút) ${human} ${dayStrFormatted} (OFF ngoài)`);
          }
        }
      }
    }

    if (notesForDetail.length) {
      changes.set(r0, { notes: notesForDetail, offErrorCount });
    }
  }

  return { changes, problematicCells };
}

/**
 * Phân tích dữ liệu OFF form để phát hiện lỗi quên check in/out và trễ
 * @param {Array} formData - Dữ liệu từ Google Form (loadOffFormData_)
 * @param {Map} nameToEmpMap - Map từ tên sang mã nhân viên
 * @param {Map} scheduleMap - Map đăng ký ca OFF (loadOffScheduleRegistrations_)
 * @param {Object} masterInfo - Thông tin master sheet
 * @param {Object} cfg - Config (morningStart, afternoonStart, lateThreshold)
 * @param {number} month - Tháng
 * @param {string} mode - 'both' | 'late' | 'missing'
 * @return {Object} { changes: Map<r0, {notes, offErrorCount}>, problematicCells: Array }
 */
function prepareOffAttendanceChanges_(formData, nameToEmpMap, scheduleMap, masterInfo, cfg, month, mode) {
  const changes = new Map();
  const problematicCells = [];
  const normalizedMode = (String(mode || 'both').toLowerCase());
  const runLate = (normalizedMode === 'both' || normalizedMode === 'late');
  const runMissing = (normalizedMode === 'both' || normalizedMode === 'missing');

  // Helper để convert session name sang human-readable
  const humanForSession = (sess) => {
    if (!sess) return 'ca';
    if (sess === 'morning') return 'ca sáng';
    if (sess === 'afternoon') return 'ca chiều';
    if (sess === 'evening') return 'ca tối';
    return 'ca';
  };

  // Group form data theo empCode và day
  const empDayData = new Map(); // Map<empCode, Map<dayStr, {morning: {in, out}, afternoon: {in, out}}>>

  for (const entry of formData) {
    const normalizedName = normalize_(entry.fullName);
    const empCode = nameToEmpMap.get(normalizedName);
    if (!empCode) continue;

    const dayStr = entry.day;
    if (!dayStr) continue;

    if (!empDayData.has(empCode)) empDayData.set(empCode, new Map());
    const dayMap = empDayData.get(empCode);
    if (!dayMap.has(dayStr)) {
      dayMap.set(dayStr, {
        morning: { in: null, out: null },
        afternoon: { in: null, out: null },
        evening: { in: null, out: null }
      });
    }

    const session = dayMap.get(dayStr)[entry.shiftType];
    if (!session) continue;

    // Lưu timestamp vào in hoặc out
    if (entry.action === 'in') {
      session.in = entry.timestamp;
    } else if (entry.action === 'out') {
      session.out = entry.timestamp;
    }
  }

  // Duyệt qua tất cả nhân viên có đăng ký ca OFF
  for (const [empCode, daySchedMap] of scheduleMap.entries()) {
    const r1 = masterInfo.empToRow.get(empCode);
    if (!r1) continue; // Không có trong master sheet
    const r0 = r1 - 1;

    let notesForDetail = [];
    let offErrorCount = 0;

    // Lấy schedule template để tính lateMinutes
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(empCode) : undefined;
    const scheduleTemplate = getEmployeeSchedule_(empCode, cfg, role);

    // Duyệt qua từng ngày có đăng ký ca OFF
    for (const [dayStr, registeredSessions] of daySchedMap.entries()) {
      const dayData = empDayData.get(empCode);
      const sessions = dayData && dayData.get(dayStr) ? dayData.get(dayStr) : {
        morning: { in: null, out: null },
        afternoon: { in: null, out: null },
        evening: { in: null, out: null }
      };

      // Duyệt qua từng ca đã đăng ký
      for (const sessionName of registeredSessions) {
        if (sessionName === '_problematic' || sessionName === '_timesCount') continue;

        const session = sessions[sessionName];
        if (!session) continue;

        const human = humanForSession(sessionName);
        const dayNum = parseInt(dayStr, 10);
        const dayStrFormatted = `${dayNum}/${month}`;

        // Tính lateMinutes trước để dùng cho cả missing và late mode. Đồng bộ với highlight: tính khi có session.in (không bắt buộc session.out).
        let lateMinutes = 0;
        if (session.in) {
          const checkInTime = session.in;
          const checkInMin = checkInTime.getHours() * 60 + checkInTime.getMinutes();

          let sessionStartMin = null;
          if (sessionName === 'morning') {
            const morningStart = scheduleTemplate.morning?.start || cfg.morningStart || SPECIAL_SCHEDULES.default.morningStart;
            sessionStartMin = timeStrToMinutes_(morningStart);
          } else if (sessionName === 'afternoon') {
            const afternoonStart = scheduleTemplate.afternoon?.start || cfg.afternoonStart || SPECIAL_SCHEDULES.default.afternoonStart;
            sessionStartMin = timeStrToMinutes_(afternoonStart);
          }

          if (sessionStartMin !== null && checkInMin > sessionStartMin) {
            lateMinutes = checkInMin - sessionStartMin;
          }
        }
        const lateMinutesRounded = Math.round(lateMinutes);
        const threshold = cfg.lateThreshold || 30;

        // 1) Xử lý QUÊN CHECK IN/OUT (tùy theo mode)
        if (runMissing) {
          if (!session.in && !session.out) {
            // Vắng hoàn toàn - không tính vào lỗi quên check in/out
            continue;
          }
          if (!session.in && session.out) {
            // Quên check in nhưng có check out
            notesForDetail.push(`- Quên check in ${human} ${dayStrFormatted} (OFF ngoài)`);
            offErrorCount++;
          } else if (session.in && !session.out) {
            // Có check in nhưng quên check out
            notesForDetail.push(`- Quên check out ${human} ${dayStrFormatted} (OFF ngoài)`);
            offErrorCount++;
          } else if (session.in && lateMinutesRounded > threshold) {
            // Trễ >= 30 phút: quy thành quên check in (chỉ xử lý trong mode missing). Đồng bộ với highlight: không bắt buộc session.out.
            notesForDetail.push(`- Check in trễ từ 30 phút trở lên (${lateMinutesRounded} phút) ${human} ${dayStrFormatted} (OFF ngoài)`);
            offErrorCount++;
          }
        }

        // 2) Xử lý TRỄ CHECK-IN (tùy theo mode). Đồng bộ với highlight: không bắt buộc session.out.
        if (runLate && session.in && lateMinutesRounded > 0 && lateMinutesRounded <= threshold) {
          // Trễ <= 30 phút (bao gồm cả = 30 phút) là lỗi TRỄ, ghi cột S
          notesForDetail.push(`- Check in trễ dưới hoặc bằng 30 phút (${lateMinutesRounded} phút) ${human} ${dayStrFormatted} (OFF ngoài)`);
        }
      }
    }

    if (notesForDetail.length) {
      changes.set(r0, { notes: notesForDetail, offErrorCount });
    }
  }

  return { changes, problematicCells };
}

/**
 * Xử lý lỗi OFF từ master sheet (các cột CB-DF) - chỉ xử lý QUÊN CHECK IN/OUT
 * @param {Object} opts - Options { dryRun, testRows }
 * @return {Object} Kết quả xử lý
 */
function applyOffAttendanceMissingOnly(opts) {
  opts = opts || {};
  const dryRun = typeof opts.dryRun === 'boolean' ? opts.dryRun : false;
  const testRows = (typeof opts.testRows === 'number' && opts.testRows > 0) ? Math.trunc(opts.testRows) : null;

  // ====== CONFIG ======
  const MASTER_FILE_ID = '1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4';
  const MASTER_SHEET_NAME = 'Chấm công th12/2025';
  const cfg = { morningStart: '08:30', afternoonStart: '13:15', cutoff: '12:00', lateThreshold: 30, maxTimesThreshold: 4 };

  Logger.log('1) Loading master sheet info...');
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  masterInfo.masterSh = masterSh;
  const month = parseMonthFromSheetName_(MASTER_SHEET_NAME) || 12;
  const headerMap = findHeaderCols_(masterInfo.header);

  Logger.log('2) Analyzing OFF attendance from sheet (missing only, chỉ theo data ô, không đăng ký ca)...');
  const result = prepareOffAttendanceChangesFromSheet_(masterInfo, cfg, month, 'missing');
  const changes = result.changes || new Map();
  const problematicCells = result.problematicCells || [];

  Logger.log(`   Computed changes=${changes.size} problematic=${problematicCells.length}`);

  // Prepare arrays for writing
  const lastEmpRow = masterInfo.lastEmpRow;
  const noteCol = headerMap.noteCol || headerMap.detail3Col || headerMap.detail2Col; // Cột W
  const offForgotCol = headerMap.offForgotCol || null; // Cột tương tự V cho OFF (nếu có)
  const noteArr = noteCol ? masterSh.getRange(1, noteCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];
  const offForgotArr = offForgotCol ? masterSh.getRange(1, offForgotCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];

  const newNote = noteArr.slice();
  const newOffForgot = offForgotArr.slice();

  // Apply changes into arrays
  // LƯU Ý: r0 trong changes là index trong offDataBlock (bắt đầu từ hàng 2)
  // - offDataBlock[0] = hàng 2 trong sheet -> r0 = 0
  // - offDataBlock[1] = hàng 3 trong sheet -> r0 = 1
  // - offDataBlock[2] = hàng 4 trong sheet -> r0 = 2
  // Nhưng noteArr bắt đầu từ hàng 1:
  // - noteArr[0] = hàng 1 (header)
  // - noteArr[1] = hàng 2 -> tương ứng với r0 = 0
  // - noteArr[2] = hàng 3 -> tương ứng với r0 = 1
  // - noteArr[3] = hàng 4 -> tương ứng với r0 = 2
  // Vậy để map r0 sang index trong noteArr: noteIndex = r0 + 1
  for (const [r0, v] of changes.entries()) {
    const allNotes = v.notes || [];
    const offErrorCount = v.offErrorCount || 0;
    
    // Chuyển đổi r0 (index trong offDataBlock) sang index trong noteArr
    const noteIndex = r0 + 1; // r0=0 -> noteIndex=1 (hàng 2), r0=1 -> noteIndex=2 (hàng 3), r0=2 -> noteIndex=3 (hàng 4)
    
    if (noteIndex < 0 || noteIndex >= newNote.length) {
      Logger.log(`Warning: noteIndex ${noteIndex} (from r0=${r0}) out of range [0, ${newNote.length - 1}]`);
      continue;
    }

    // Ghi note vào cột W (nối chuỗi) - chỉ xử lý quên check in/out
    if (noteCol && allNotes.length) {
      const prev = String(newNote[noteIndex] || '').trim();
      newNote[noteIndex] = (prev ? prev + '\n' : '') + allNotes.join('\n');
    }

    // Cộng số lỗi vào cột đếm OFF (nếu có)
    if (offForgotCol) {
      const prev = Number(newOffForgot[noteIndex] || 0);
      newOffForgot[noteIndex] = prev + offErrorCount;
    }
  }

  // Preview
  if (dryRun) {
    Logger.log('applyOffAttendanceMissingOnly (dryRun) preview sample:');
    let i = 0;
    for (const [r0, v] of changes.entries()) {
      const noteIndex = r0 + 1;
      const actualRow = noteIndex + 1;
      Logger.log(`PREVIEW Row ${actualRow} (r0=${r0}, noteIndex=${noteIndex}): ${v.notes.join('; ')} (count=${v.offErrorCount})`);
      if (++i >= 50) break;
    }
    return { changesCount: changes.size, problematicCellsCount: problematicCells.length };
  }

  // If testRows specified -> write per-row for first N changes (safe)
  const changeEntries = Array.from(changes.entries());
  if (testRows) {
    Logger.log(`applyOffAttendanceMissingOnly: testRows write mode N=${testRows} (per-row writes)`);
    const slice = changeEntries.slice(0, testRows);
    slice.forEach(([r0, v]) => {
      const noteIndex = r0 + 1;
      const rowNum = noteIndex + 1;
      if (noteCol) {
        const prev = String(noteArr[noteIndex] || '').trim();
        masterSh.getRange(rowNum, noteCol).setValue((prev ? prev + '\n' : '') + v.notes.join('\n'));
      }
      if (offForgotCol) {
        masterSh.getRange(rowNum, offForgotCol).setValue(Number(offForgotArr[noteIndex] || 0) + Number(v.offErrorCount || 0));
      }
      Logger.log(`WROTE row ${rowNum} (r0=${r0}, noteIndex=${noteIndex})`);
    });
    return { changesCount: changeEntries.length, written: slice.length };
  }

  // Otherwise full commit: write columns back
  const writes = [];
  if (noteCol) writes.push({ range: masterSh.getRange(1, noteCol, lastEmpRow, 1), values: newNote.map(x => [x || '']) });
  if (offForgotCol) writes.push({ range: masterSh.getRange(1, offForgotCol, lastEmpRow, 1), values: newOffForgot.map(x => [x || 0]) });

  writes.forEach(w => w.range.setValues(w.values));
  Logger.log(`applyOffAttendanceMissingOnly: full commit wrote ${writes.length} ranges`);
  return { changesCount: changes.size, written: writes.length };
}

/**
 * Xử lý lỗi TRỄ OFF từ master sheet (các cột CB-DF) - chỉ xử lý TRỄ
 * Trễ <= 30 phút: ghi vào cột S với (OFF ngoài), đếm ở cột P (TRỄ OFF NGOÀI (nhập tay))
 * Trễ > 30 phút: quy thành quên check in, KHÔNG ghi vào cột S
 * @param {Object} opts - Options { dryRun, testRows }
 * @return {Object} Kết quả xử lý
 */
function applyOffAttendanceLateOnly(opts) {
  opts = opts || {};
  const dryRun = typeof opts.dryRun === 'boolean' ? opts.dryRun : false;
  const testRows = (typeof opts.testRows === 'number' && opts.testRows > 0) ? Math.trunc(opts.testRows) : null;

  // ====== CONFIG ======
  const MASTER_FILE_ID = '1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4';
  const MASTER_SHEET_NAME = 'Chấm công th12/2025';
  const cfg = { morningStart: '08:30', afternoonStart: '13:15', cutoff: '12:00', lateThreshold: 30, maxTimesThreshold: 4 };

  Logger.log('1) Loading master sheet info...');
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  masterInfo.masterSh = masterSh;
  const month = parseMonthFromSheetName_(MASTER_SHEET_NAME) || 12;
  const headerMap = findHeaderCols_(masterInfo.header);

  Logger.log('2) Analyzing OFF attendance from sheet (late only, chỉ theo data ô, không đăng ký ca)...');
  const result = prepareOffAttendanceChangesFromSheet_(masterInfo, cfg, month, 'late');
  const changes = result.changes || new Map();
  const problematicCells = result.problematicCells || [];

  Logger.log(`   Computed changes=${changes.size} problematic=${problematicCells.length}`);

  // Prepare arrays for writing
  const lastEmpRow = masterInfo.lastEmpRow;
  const lateNoteCol = headerMap.detail2Col || null; // Cột S (19) - ghi note TRỄ
  const totalLateCol = headerMap.offLateCol || null; // Cột P (16) - đếm số lượng trễ OFF NGOÀI (nhập tay)
  const lateNoteArr = lateNoteCol ? masterSh.getRange(1, lateNoteCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];
  const totalLateArr = totalLateCol ? masterSh.getRange(1, totalLateCol, lastEmpRow, 1).getValues().map(r => r[0]) : [];

  const newLateNote = lateNoteArr.slice();
  const newTotalLate = totalLateArr.slice();

  // Apply changes into arrays
  // LƯU Ý: r0 trong changes là index trong offDataBlock (bắt đầu từ hàng 2)
  // - offDataBlock[0] = hàng 2 trong sheet -> r0 = 0
  // - offDataBlock[1] = hàng 3 trong sheet -> r0 = 1
  // - offDataBlock[2] = hàng 4 trong sheet -> r0 = 2
  // Nhưng lateNoteArr bắt đầu từ hàng 1:
  // - lateNoteArr[0] = hàng 1 (header)
  // - lateNoteArr[1] = hàng 2 -> tương ứng với r0 = 0
  // - lateNoteArr[2] = hàng 3 -> tương ứng với r0 = 1
  // - lateNoteArr[3] = hàng 4 -> tương ứng với r0 = 2
  // Vậy để map r0 sang index trong lateNoteArr: noteIndex = r0 + 1
  for (const [r0, v] of changes.entries()) {
    const allNotes = v.notes || [];
    
    // Chỉ lấy các note TRỄ (trễ <= 30 phút), bỏ qua note quên check in/out
    const lateNotes = allNotes.filter(n => {
      if (typeof n !== 'string') return false;
      const nLower = n.toLowerCase();
      // Chỉ lấy note có "trễ" và không có "quên check"
      return nLower.includes('trễ') && !nLower.includes('quên check');
    });
    
    // Chuyển đổi r0 (index trong offDataBlock) sang index trong lateNoteArr
    const noteIndex = r0 + 1; // r0=0 -> noteIndex=1 (hàng 2), r0=1 -> noteIndex=2 (hàng 3), r0=2 -> noteIndex=3 (hàng 4)
    
    if (noteIndex < 0 || noteIndex >= newLateNote.length) {
      Logger.log(`Warning: noteIndex ${noteIndex} (from r0=${r0}) out of range [0, ${newLateNote.length - 1}]`);
      continue;
    }

    // Ghi note TRỄ vào cột S (ghi đè, không append)
    if (lateNoteCol && lateNotes.length > 0) {
      newLateNote[noteIndex] = lateNotes.join('\n');
    } else if (lateNoteCol) {
      // Nếu không có note trễ, xóa nội dung cũ (ghi đè bằng rỗng)
      newLateNote[noteIndex] = '';
    }

    // Đếm số lượng trễ vào cột P (TRỄ OFF NGOÀI (nhập tay))
    if (totalLateCol) {
      const prev = Number(newTotalLate[noteIndex] || 0);
      newTotalLate[noteIndex] = prev + lateNotes.length; // Cộng dồn
    }
  }

  // Preview
  if (dryRun) {
    Logger.log('applyOffAttendanceLateOnly (dryRun) preview sample:');
    let i = 0;
    for (const [r0, v] of changes.entries()) {
      const noteIndex = r0 + 1;
      const actualRow = noteIndex + 1;
      const lateNotes = (v.notes || []).filter(n => {
        if (typeof n !== 'string') return false;
        const nLower = n.toLowerCase();
        return nLower.includes('trễ') && !nLower.includes('quên check');
      });
      Logger.log(`PREVIEW Row ${actualRow} (r0=${r0}, noteIndex=${noteIndex}): ${lateNotes.join('; ')} (count=${lateNotes.length})`);
      if (++i >= 50) break;
    }
    return { changesCount: changes.size, problematicCellsCount: problematicCells.length };
  }

  // If testRows specified -> write per-row for first N changes (safe)
  const changeEntries = Array.from(changes.entries());
  if (testRows) {
    Logger.log(`applyOffAttendanceLateOnly: testRows write mode N=${testRows} (per-row writes)`);
    const slice = changeEntries.slice(0, testRows);
    slice.forEach(([r0, v]) => {
      const noteIndex = r0 + 1;
      const rowNum = noteIndex + 1;
      const lateNotes = (v.notes || []).filter(n => {
        if (typeof n !== 'string') return false;
        const nLower = n.toLowerCase();
        return nLower.includes('trễ') && !nLower.includes('quên check');
      });
      
      if (lateNoteCol) {
        masterSh.getRange(rowNum, lateNoteCol).setValue(lateNotes.length > 0 ? lateNotes.join('\n') : '');
      }
      if (totalLateCol) {
        const prev = Number(totalLateArr[noteIndex] || 0);
        masterSh.getRange(rowNum, totalLateCol).setValue(prev + lateNotes.length);
      }
      Logger.log(`WROTE row ${rowNum} (r0=${r0}, noteIndex=${noteIndex}): ${lateNotes.length} late notes`);
    });
    return { changesCount: changeEntries.length, written: slice.length };
  }

  // Otherwise full commit: write columns back
  const writes = [];
  if (lateNoteCol) writes.push({ range: masterSh.getRange(1, lateNoteCol, lastEmpRow, 1), values: newLateNote.map(x => [x || '']) });
  if (totalLateCol) writes.push({ range: masterSh.getRange(1, totalLateCol, lastEmpRow, 1), values: newTotalLate.map(x => [x || 0]) });

  writes.forEach(w => w.range.setValues(w.values));
  Logger.log(`applyOffAttendanceLateOnly: full commit wrote ${writes.length} ranges`);
  return { changesCount: changes.size, written: writes.length };
}

/**
 * IMPORT GOOGLE FORM CHẤM CÔNG ONLINE -> SHEET TỔNG (Cột DO -> ES)
 * Xử lý CA ONLINE: fill vào cột DO -> ES với format "onl ca sáng", "onl ca chiều", "onl 2 ca"
 * Quy tắc 24h: ngày ghi nhận lấy từ cột G (EM CHẤM CÔNG CHO NGÀY NÀO). Chỉ ghi khi timestamp (A) cùng ngày với cột G; check-out qua ngày hôm sau → bỏ, ô ngày hôm sau trống.
 */
function importOnlineFormToMaster() {
  // ====== CONFIG ======
  const FORM_FILE_ID = "1GATYUk6jMyNIRyDI1FxH-I9ix6NafyE3PwJn2ptHJ1k"; // File chứa form responses
  const FORM_SHEET_NAME = "Form Responses 1";
  
  const MASTER_FILE_ID = "1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4";
  const MASTER_SHEET_NAME = "Chấm công th12/2025";
  
  const MASTER_EMP_COL = 2;     // cột mã nhân viên (B)
  const MASTER_HEADER_ROW = 1;  // hàng chứa số ngày 1..31
  const ONLINE_START_COL = 119; // Cột DO (119 = DO)
  const ONLINE_END_COL = 149;   // Cột ES (149 = ES) = 31 cột
  
  // Mapping cột form: G = EM CHẤM CÔNG CHO NGÀY NÀO (ngày ca), H = CA LÀM VIỆC CỦA EM (0-based)
  const FORM_COL_TIMESTAMP = 0;      // A: Dấu thời gian
  const FORM_COL_EMAIL = 1;           // B: Email
  const FORM_COL_EMP_CODE = 2;        // C: Mã nhân viên
  const FORM_COL_NAME = 3;            // D: Họ và tên
  const FORM_COL_TEAM = 4;            // E: Team
  const FORM_COL_DATE = 6;            // G: EM CHẤM CÔNG CHO NGÀY NÀO (ngày ca làm việc → ô ghi + quy tắc 24h)
  const FORM_COL_SHIFT = 7;           // H: CA LÀM VIỆC CỦA EM
  const FORM_COL_PROOF = 8;           // I: Minh chứng
  const FORM_COL_TYPE = 9;            // J: EM CHẤM CÔNG CHO HÌNH THỨC (CA ONLINE / CA OFFLINE)
  const FORM_COL_WORK_TYPE = 8;       // I: Hình thức làm việc
  
  Logger.log("1) Opening form responses sheet...");
  const formSS = SpreadsheetApp.openById(FORM_FILE_ID);
  const formSh = formSS.getSheetByName(FORM_SHEET_NAME);
  if (!formSh) throw new Error("Không tìm thấy sheet form: " + FORM_SHEET_NAME);
  
  const formValues = formSh.getDataRange().getValues();
  Logger.log(`   Loaded ${formValues.length} rows from form`);
  
  // Debug: Log tất cả header để xem cấu trúc form
  if (formValues.length > 0) {
    const headerRow = formValues[0];
    Logger.log(`   Debug: All headers (${headerRow.length} columns):`);
    for (let c = 0; c < headerRow.length; c++) {
      Logger.log(`     Col ${c}: "${headerRow[c]}"`);
    }
  }
  
  // Tự động tìm cột TYPE từ header row (row 0)
  // Cột TYPE là cột J với header "EM CHẤM CÔNG CHO HÌNH THỨC:"
  let actualTypeCol = FORM_COL_TYPE;
  let foundTypeCol = false;
  if (formValues.length > 0) {
    const headerRow = formValues[0];
    for (let c = 0; c < headerRow.length; c++) {
      const headerText = String(headerRow[c] || "").toLowerCase().trim();
      // Tìm cột có chứa "em chấm công cho hình thức" hoặc "hình thức"
      if (headerText.includes("em chấm công cho hình thức") || 
          headerText.includes("chấm công cho hình thức") ||
          (headerText.includes("hình thức") && headerText.includes("chấm công"))) {
        actualTypeCol = c;
        foundTypeCol = true;
        Logger.log(`   Found TYPE column at index ${c} (header: "${headerRow[c]}")`);
        break;
      }
    }
    // Nếu không tìm thấy, thử tìm bằng "ca online" hoặc "ca offline" nhưng KHÔNG phải "minh chứng"
    if (!foundTypeCol) {
      for (let c = 0; c < headerRow.length; c++) {
        const headerText = String(headerRow[c] || "").toLowerCase().trim();
        if ((headerText.includes("ca online") || headerText.includes("ca offline")) &&
            !headerText.includes("minh chứng") && !headerText.includes("minh chung") &&
            !headerText.includes("proof") && !headerText.includes("drive")) {
          actualTypeCol = c;
          foundTypeCol = true;
          Logger.log(`   Found TYPE column at index ${c} (header: "${headerRow[c]}") by 'online/offline' keyword`);
          break;
        }
      }
    }
    if (!foundTypeCol) {
      Logger.log(`   WARNING: Could not find TYPE column automatically, using default index ${FORM_COL_TYPE} (column J)`);
    }
  }
  
  // Tự động tìm cột EMP_CODE từ header row
  let actualEmpCol = FORM_COL_EMP_CODE;
  if (formValues.length > 0) {
    const headerRow = formValues[0];
    for (let c = 0; c < headerRow.length; c++) {
      const headerText = String(headerRow[c] || "").toLowerCase().trim();
      if (headerText.includes("mã nhân viên") || headerText.includes("ma nhan vien") ||
          headerText.includes("employee") || headerText.includes("code")) {
        actualEmpCol = c;
        Logger.log(`   Found EMP_CODE column at index ${c} (header: "${headerRow[c]}")`);
        break;
      }
    }
  }
  
  // Tự động tìm cột SHIFT từ header row
  let actualShiftCol = FORM_COL_SHIFT;
  if (formValues.length > 0) {
    const headerRow = formValues[0];
    for (let c = 0; c < headerRow.length; c++) {
      const headerText = String(headerRow[c] || "").toLowerCase().trim();
      if (headerText.includes("ca làm việc") || headerText.includes("ca lam viec") ||
          headerText.includes("shift") || headerText.includes("ca sáng") || 
          headerText.includes("ca chiều") || headerText.includes("ca sang") ||
          headerText.includes("ca chieu")) {
        actualShiftCol = c;
        Logger.log(`   Found SHIFT column at index ${c} (header: "${headerRow[c]}")`);
        break;
      }
    }
  }
  
  // ====== 2) PARSE FORM DATA - CHỈ LẤY CA ONLINE VÀ THÁNG 12/2025 ======
  Logger.log("2) Parsing form data (CA ONLINE only, month 12/2025)...");
  // Map: empCode -> Map<dayStr, {morning: {in, out}, afternoon: {in, out}}>
  const onlineByEmpDay = new Map();
  // Mã nhân viên có thể là MHxxxx, LL082, HN045, etc. - không giới hạn format
  // Nới lỏng regex: cho phép chữ cái, số, và một số ký tự đặc biệt thường gặp
  // Loại bỏ các ký tự không hợp lệ như khoảng trắng, ký tự đặc biệt lạ
  const empRegex = /^[A-Z0-9_-]{2,}$/i;
  
  let skippedType = 0;
  let skippedEmp = 0;
  let skippedDate = 0;
  let skippedMonth = 0;
  let skippedTime = 0;
  let skippedCrossDay = 0;
  let processed = 0;
  const skippedEmpCodes = new Set(); // Để log các mã bị skip
  const validEmpCodes = new Set(); // Để log các mã hợp lệ
  const validButSkipped = []; // Để log các dòng có mã hợp lệ nhưng bị skip vì lý do khác
  
  // Debug: Log các giá trị unique trong cột TYPE để xem có gì
  const uniqueTypes = new Set();
  for (let r = 1; r < Math.min(100, formValues.length); r++) {
    const typeVal = String(formValues[r][actualTypeCol] || "").trim();
    if (typeVal) uniqueTypes.add(typeVal);
  }
  Logger.log(`   Debug: Found ${uniqueTypes.size} unique TYPE values in first 100 rows:`);
  Array.from(uniqueTypes).slice(0, 20).forEach((val, idx) => {
    Logger.log(`     ${idx + 1}. "${val}"`);
  });
  
  // Debug: Log vài giá trị type đầu tiên để kiểm tra
  Logger.log("   Debug: Checking first 10 rows for type values...");
  Logger.log(`   Using TYPE column index: ${actualTypeCol}, EMP_CODE column index: ${actualEmpCol}, SHIFT column index: ${actualShiftCol}`);
  for (let debugR = 1; debugR <= Math.min(10, formValues.length - 1); debugR++) {
    const debugRow = formValues[debugR];
    const debugType = String(debugRow[actualTypeCol] || "").trim();
    const debugEmp = String(debugRow[actualEmpCol] || "").trim();
    const debugShift = debugRow[actualShiftCol];
    const debugShiftStr = debugShift instanceof Date ? debugShift.toString() : String(debugShift || "").trim();
    // Log thêm các cột xung quanh để debug
    const debugTypePrev = String(debugRow[actualTypeCol - 1] || "").trim();
    const debugTypeNext = String(debugRow[actualTypeCol + 1] || "").trim();
    Logger.log(`   Row ${debugR + 1}: type[${actualTypeCol}]="${debugType}", emp="${debugEmp}", shift[${actualShiftCol}]="${debugShiftStr}"`);
  }
  
  // Bỏ qua header row (row 0)
  for (let r = 1; r < formValues.length; r++) {
    const row = formValues[r];
    
    // Chỉ xử lý CA ONLINE - so sánh không phân biệt hoa thường và trim
    const type = String(row[actualTypeCol] || "").trim();
    const typeUpper = type.toUpperCase();
    // Cho phép các biến thể: "CA ONLINE", "ca online", "CA ONLINE ", etc.
    if (typeUpper !== "CA ONLINE") {
      skippedType++;
      continue;
    }
    
    // Lấy mã nhân viên từ cột đã tìm được
    const empCodeRaw = String(row[actualEmpCol] || "").trim();
    if (!empCodeRaw) {
      skippedEmp++;
      skippedEmpCodes.add("(empty)");
      continue; // Bỏ qua nếu không có mã
    }
    if (!empRegex.test(empCodeRaw)) {
      skippedEmp++;
      skippedEmpCodes.add(empCodeRaw);
      continue; // Bỏ qua nếu mã không hợp lệ
    }
    const empCode = empCodeRaw.toUpperCase();
    validEmpCodes.add(empCode);
    
    // Lấy ngày ca làm việc từ cột G (EM CHẤM CÔNG CHO NGÀY NÀO). Quy tắc 24h: timestamp (A) phải cùng ngày → khác ngày thì bỏ (ô 02/12 trống).
    const dateValue = row[FORM_COL_DATE];
    let dayStr = null;
    let isDec2025 = false;
    let parsedYear = null;
    let parsedMonth = null;
    let parsedDay = null;
    
    if (dateValue instanceof Date) {
      parsedYear = dateValue.getFullYear();
      parsedMonth = dateValue.getMonth() + 1; // getMonth() trả về 0-11
      parsedDay = dateValue.getDate();
      if (parsedYear === 2025 && parsedMonth === 12) {
        isDec2025 = true;
        dayStr = String(parsedDay);
      }
    } else {
      const dateStr = String(dateValue || "").trim();
      // Parse từ string "DD/MM/YYYY" hoặc "D/M/YYYY" (có thể có thêm giờ "DD/MM/YYYY HH:MM:SS")
      // Regex sẽ match phần date trước dấu cách hoặc ký tự không phải số
      const dateMatch = dateStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s|$)/);
      if (dateMatch) {
        parsedDay = dateMatch[1];
        parsedMonth = dateMatch[2];
        parsedYear = dateMatch[3];
        // Kiểm tra tháng 12 (parseInt để xử lý cả "12" và "12")
        if (parsedYear === "2025" && parseInt(parsedMonth) === 12) {
          isDec2025 = true;
          dayStr = parsedDay;
        }
      } else {
        // Thử parse format khác: "YYYY-MM-DD" hoặc "DD-MM-YYYY" (có thể có thêm giờ)
        const dateMatch2 = dateStr.match(/(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})(?:\s|$)/);
        if (dateMatch2) {
          // Format YYYY-MM-DD
          parsedYear = dateMatch2[1];
          parsedMonth = dateMatch2[2];
          parsedDay = dateMatch2[3];
          if (parsedYear === "2025" && parseInt(parsedMonth) === 12) {
            isDec2025 = true;
            dayStr = parsedDay;
          }
        } else {
          // Thử format DD-MM-YYYY
          const dateMatch3 = dateStr.match(/(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})(?:\s|$)/);
          if (dateMatch3) {
            parsedDay = dateMatch3[1];
            parsedMonth = dateMatch3[2];
            parsedYear = dateMatch3[3];
            if (parsedYear === "2025" && parseInt(parsedMonth) === 12) {
              isDec2025 = true;
              dayStr = parsedDay;
            }
          }
        }
      }
    }
    
    // Debug: Log vài dòng đầu để xem parse ngày và shift type
    if (processed < 5 && empCode) {
      const debugShift = row[actualShiftCol];
      const debugShiftStr = debugShift instanceof Date ? debugShift.toString() : String(debugShift || "").trim();
      Logger.log(`   DEBUG Row ${r + 1}, emp=${empCode}: dateValue="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay}, isDec2025=${isDec2025}, shift[${actualShiftCol}]="${debugShiftStr}"`);
    }
    
    // Chỉ xử lý dữ liệu từ tháng 12/2025
    if (!isDec2025) {
      skippedMonth++;
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, date="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay} (not Dec 2025)`);
      }
      continue;
    }
    
    // Validate dayStr - chuyển sang số để so sánh chính xác và chuẩn hóa format
    const dayNum = parseInt(dayStr);
    if (!dayStr || isNaN(dayNum) || dayNum < 1 || dayNum > 31) {
      skippedDate++;
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, dayStr="${dayStr}" (invalid day)`);
      }
      continue;
    }
    // Chuẩn hóa dayStr: đảm bảo là string "1", "2", ..., "31" (không có leading zero)
    dayStr = String(dayNum);
    
    // Lấy loại ca và timestamp
    // Theo yêu cầu hiện tại: ưu tiên cột H (index 7) nếu chứa "check in/out"; fallback sang cột SHIFT đã detect; cuối cùng fallback sang cột G mặc định.
    const isShiftText_ = (v) => {
      if (!v) return false;
      if (v instanceof Date) return false;
      const s = String(v || '').toLowerCase();
      return s.includes('check in') || s.includes('check out') || s.includes('checkin') || s.includes('checkout');
    };
    let shiftTypeRaw = row[actualShiftCol];
    if (isShiftText_(row[7])) shiftTypeRaw = row[7]; // cột H (ưu tiên nếu đúng dữ liệu phân loại)
    else if (isShiftText_(row[actualShiftCol])) shiftTypeRaw = row[actualShiftCol];
    else if (isShiftText_(row[FORM_COL_SHIFT])) shiftTypeRaw = row[FORM_COL_SHIFT];

    if (shiftTypeRaw instanceof Date) {
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, day=${dayStr}: Shift value is Date object. Skipping.`);
      continue;
    }
    const shiftType = String(shiftTypeRaw || "").trim();

    const timestamp = row[FORM_COL_TIMESTAMP];

    // Quy tắc hiệu lực 24h (theo ngày chấm công): timestamp phải cùng ngày với dateValue đã parse.
    // Nếu check-out qua ngày hôm sau -> không tính (skip entry).
    try {
      const tz = Session.getScriptTimeZone();
      const workDateObj = new Date(Number(parsedYear), Number(parsedMonth) - 1, Number(parsedDay), 12, 0, 0);
      const workKey = Utilities.formatDate(workDateObj, tz, 'yyyy-MM-dd');
      let tsKey = null;
      if (timestamp instanceof Date && !isNaN(timestamp.getTime())) {
        tsKey = Utilities.formatDate(timestamp, tz, 'yyyy-MM-dd');
      } else {
        const tsStr = String(timestamp || '').trim();
        const m1 = tsStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
        const m2 = !m1 ? tsStr.match(/(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})/) : null;
        if (m1) {
          const d = Number(m1[1]), mo = Number(m1[2]), y = Number(m1[3]);
          const dt = new Date(y, mo - 1, d, 12, 0, 0);
          tsKey = Utilities.formatDate(dt, tz, 'yyyy-MM-dd');
        } else if (m2) {
          const y = Number(m2[1]), mo = Number(m2[2]), d = Number(m2[3]);
          const dt = new Date(y, mo - 1, d, 12, 0, 0);
          tsKey = Utilities.formatDate(dt, tz, 'yyyy-MM-dd');
        }
      }
      if (!tsKey || tsKey !== workKey) {
        skippedCrossDay++;
        if (processed < 5 && empCode) {
          Logger.log(`   Skip cross-day entry Row ${r + 1}, emp=${empCode}, day=${dayStr}: workKey=${workKey}, tsKey=${tsKey}, shift="${shiftType}"`);
        }
        continue;
      }
    } catch (e) {
      skippedCrossDay++;
      Logger.log(`   Skip entry Row ${r + 1} due to cross-day check error: ${e && e.message ? e.message : e}`);
      continue;
    }
    
    // Parse timestamp để lấy giờ
    let timeStr = null;
    if (timestamp instanceof Date) {
      const hours = timestamp.getHours();
      const minutes = timestamp.getMinutes();
      timeStr = `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}`;
    } else {
      // Parse từ string "DD/MM/YYYY HH:MM:SS" hoặc "DD/MM/YYYY H:MM:SS"
      const timeMatch = String(timestamp || "").match(/(\d{1,2}):(\d{2}):\d{2}/);
      if (timeMatch) {
        const h = timeMatch[1];
        const m = timeMatch[2];
        timeStr = `${h.padStart(2, '0')}:${m}`;
      }
    }
    if (!timeStr) {
      skippedTime++;
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, day=${dayStr}, timestamp="${timestamp}" (cannot parse)`);
      }
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, day=${dayStr}: Cannot parse timestamp: ${timestamp}`);
      continue;
    }
    
    processed++;
    
    // Khởi tạo map nếu chưa có
    if (!onlineByEmpDay.has(empCode)) {
      onlineByEmpDay.set(empCode, new Map());
    }
    const dayMap = onlineByEmpDay.get(empCode);
    if (!dayMap.has(dayStr)) {
      dayMap.set(dayStr, { morning: { in: null, out: null }, afternoon: { in: null, out: null } });
    }
    const dayData = dayMap.get(dayStr);
    
    // Phân loại ca và gán vào đúng vị trí
    // Sử dụng toLowerCase() để match không phân biệt hoa thường
    const shiftLower = shiftType.toLowerCase();
    if (shiftLower.includes("check in ca sáng") || shiftLower.includes("check in ca sang")) {
      dayData.morning.in = timeStr;
    } else if (shiftLower.includes("check out ca sáng") || shiftLower.includes("check out ca sang")) {
      dayData.morning.out = timeStr;
    } else if (shiftLower.includes("check in ca chiều") || shiftLower.includes("check in ca chieu")) {
      dayData.afternoon.in = timeStr;
    } else if (shiftLower.includes("check out ca chiều") || shiftLower.includes("check out ca chieu")) {
      dayData.afternoon.out = timeStr;
    } else {
      // Log các loại ca không match để debug - log cả giá trị gốc để xem có phải Date object không
      const shiftTypeDebug = row[actualShiftCol];
      const shiftTypeDebugStr = shiftTypeDebug instanceof Date ? shiftTypeDebug.toString() : String(shiftTypeDebug || "").trim();
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, day=${dayStr}: Unknown shift type: "${shiftType}" (raw value: "${shiftTypeDebugStr}", column ${actualShiftCol})`);
    }
    // Bỏ qua "Check in ca tối" vì chỉ xử lý ca sáng và ca chiều
  }
  
  Logger.log(`   Parsed ${onlineByEmpDay.size} employees with online check-in data`);
  Logger.log(`   Stats: processed=${processed}, skippedType=${skippedType}, skippedEmp=${skippedEmp}, skippedMonth=${skippedMonth}, skippedDate=${skippedDate}, skippedTime=${skippedTime}, skippedCrossDay=${skippedCrossDay}`);
  
  // Log các dòng có mã hợp lệ nhưng bị skip
  if (validButSkipped.length > 0) {
    Logger.log(`   Valid codes but skipped (samples):`);
    validButSkipped.forEach((msg, idx) => {
      Logger.log(`     ${idx + 1}. ${msg}`);
    });
  }
  
  // Debug: Log các mã hợp lệ và bị skip
  Logger.log(`   Valid employee codes found: ${validEmpCodes.size}`);
  if (validEmpCodes.size > 0 && validEmpCodes.size <= 50) {
    Logger.log(`   Valid codes: ${Array.from(validEmpCodes).join(", ")}`);
  } else if (validEmpCodes.size > 50) {
    Logger.log(`   Valid codes (first 50): ${Array.from(validEmpCodes).slice(0, 50).join(", ")}`);
  }
  
  Logger.log(`   Skipped employee codes: ${skippedEmpCodes.size}`);
  if (skippedEmpCodes.size > 0 && skippedEmpCodes.size <= 30) {
    Logger.log(`   Skipped codes (samples): ${Array.from(skippedEmpCodes).slice(0, 30).join(", ")}`);
  } else if (skippedEmpCodes.size > 30) {
    Logger.log(`   Skipped codes (first 30): ${Array.from(skippedEmpCodes).slice(0, 30).join(", ")}`);
  }
  
  // ====== 3) OPEN MASTER SHEET ======
  Logger.log("3) Opening master sheet...");
  const masterSS = SpreadsheetApp.openById(MASTER_FILE_ID);
  const masterSh = masterSS.getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) throw new Error("Không tìm thấy sheet tổng: " + MASTER_SHEET_NAME);
  
  const masterInfo = buildMasterInfo_(masterSh, MASTER_EMP_COL, MASTER_HEADER_ROW);
  const { empToRow, colByDay, minDayCol } = masterInfo;
  
  // Kiểm tra và đảm bảo mapping ngày -> cột đúng: ngày 1 -> DO (119), ngày 2 -> DP (120), ...
  const colOfDay1 = colByDay.get("1");
  if (colOfDay1 && colOfDay1 !== ONLINE_START_COL) {
    Logger.log(`   WARNING: Day 1 is mapped to column ${colOfDay1}, but expected ${ONLINE_START_COL} (DO). Adjusting mapping...`);
  }
  
  // ====== 4) BUILD DATA TO WRITE ======
  Logger.log("4) Building data to write (columns DO-ES)...");
  Logger.log(`   Mapping: Day 1 -> Column ${colOfDay1 || 'NOT FOUND'}, ONLINE_START_COL=${ONLINE_START_COL} (DO)`);
  const lastEmpRow = masterInfo.lastEmpRow;
  const dayColsCount = ONLINE_END_COL - ONLINE_START_COL + 1; // 31 cột
  
  // Khởi tạo mảng 2D: [row][col] = value
  // Chỉ khởi tạo cho các hàng dữ liệu (từ hàng 2 đến lastEmpRow), bỏ qua hàng 1 (header)
  // onlineBlock[0] tương ứng với hàng 2 trong sheet, onlineBlock[1] tương ứng với hàng 3, ...
  const dataRowCount = lastEmpRow - 1; // Số hàng dữ liệu (bỏ qua hàng header)
  const onlineBlock = [];
  for (let r = 0; r < dataRowCount; r++) {
    onlineBlock[r] = new Array(dayColsCount).fill("");
  }
  
  let updatedCells = 0;
  const notFound = [];
  
  for (const [empCode, dayMap] of onlineByEmpDay.entries()) {
    const row1 = empToRow.get(empCode);
    if (!row1) {
      notFound.push(empCode);
      continue;
    }
    // Bỏ qua hàng 1 (header) - chỉ xử lý từ hàng 2 trở đi
    if (row1 === 1) {
      Logger.log(`   WARNING: Skipping row 1 (header) for emp ${empCode}`);
      continue;
    }
    // row1 là hàng trong sheet (2, 3, 4, ...), chuyển sang index trong onlineBlock (0, 1, 2, ...)
    const r0 = row1 - 2; // Hàng 2 -> index 0, hàng 3 -> index 1, ...
    
    for (const [dayStr, dayData] of dayMap.entries()) {
      const dayNum = parseInt(dayStr);
      if (isNaN(dayNum) || dayNum < 1 || dayNum > 31) {
        Logger.log(`   WARNING: Invalid dayStr "${dayStr}"`);
        continue;
      }
      
      // Tính cột đích trực tiếp: ngày 1 -> DO (119), ngày 2 -> DP (120), ..., ngày 31 -> ES (149)
      // ONLINE_START_COL = 119 (DO), ngày 1 -> offset 0 -> cột 119, ngày 2 -> offset 1 -> cột 120, ...
      const onlineC0 = dayNum - 1; // 0-based: ngày 1 -> 0, ngày 2 -> 1, ..., ngày 31 -> 30
      
      // Validate: offset phải từ 0 đến 30 (31 cột: DO=0, DP=1, ..., ES=30)
      if (onlineC0 < 0 || onlineC0 >= dayColsCount) {
        Logger.log(`   WARNING: Day ${dayStr} (dayNum=${dayNum}) offset ${onlineC0} is outside range [0, ${dayColsCount-1}]`);
        continue;
      }
      
      // Validate: r0 phải trong phạm vi onlineBlock
      if (r0 < 0 || r0 >= dataRowCount) {
        Logger.log(`   WARNING: Row index ${r0} (row ${row1}) is outside range [0, ${dataRowCount-1}]`);
        continue;
      }
      
      // Tính cột thực tế trong sheet để ghi
      const targetCol = ONLINE_START_COL + onlineC0; // Ngày 1 -> 119, ngày 2 -> 120, ..., ngày 31 -> 149
      
      // Tạo format text dựa trên dữ liệu
      const parts = [];
      const hasMorning = dayData.morning.in || dayData.morning.out;
      const hasAfternoon = dayData.afternoon.in || dayData.afternoon.out;
      
      if (hasMorning && hasAfternoon) {
        // Có cả 2 ca -> "onl 2 ca"
        const morningTimes = [];
        if (dayData.morning.in) morningTimes.push(dayData.morning.in);
        if (dayData.morning.out) morningTimes.push(dayData.morning.out);
        const afternoonTimes = [];
        if (dayData.afternoon.in) afternoonTimes.push(dayData.afternoon.in);
        if (dayData.afternoon.out) afternoonTimes.push(dayData.afternoon.out);
        
        const allTimes = [...morningTimes, ...afternoonTimes];
        onlineBlock[r0][onlineC0] = allTimes.join("\n") + "\nonl 2 ca";
      } else if (hasMorning) {
        // Chỉ có ca sáng
        const morningTimes = [];
        if (dayData.morning.in) morningTimes.push(dayData.morning.in);
        if (dayData.morning.out) morningTimes.push(dayData.morning.out);
        onlineBlock[r0][onlineC0] = morningTimes.join("\n") + "\nonl ca sáng";
      } else if (hasAfternoon) {
        // Chỉ có ca chiều
        const afternoonTimes = [];
        if (dayData.afternoon.in) afternoonTimes.push(dayData.afternoon.in);
        if (dayData.afternoon.out) afternoonTimes.push(dayData.afternoon.out);
        onlineBlock[r0][onlineC0] = afternoonTimes.join("\n") + "\nonl ca chiều";
      }
      
      if (onlineBlock[r0][onlineC0]) {
        updatedCells++;
        // Debug: Log vài cell đầu để kiểm tra mapping
        if (updatedCells <= 5) {
          Logger.log(`   DEBUG: Writing day ${dayStr} (col ${targetCol}, offset ${onlineC0}) for emp ${empCode} at row ${row1}`);
        }
      }
    }
  }
  
  Logger.log(`   Prepared ${updatedCells} cells to update`);
  if (notFound.length) {
    Logger.log(`   Không tìm thấy ${notFound.length} mã trong sheet tổng (samples): ${notFound.slice(0, 20).join(", ")}`);
    if (notFound.length > 20) {
      Logger.log(`   ... và ${notFound.length - 20} mã khác`);
    }
  }
  
  // ====== 5) WRITE TO MASTER SHEET ======
  if (updatedCells > 0) {
    Logger.log("5) Writing to master sheet (columns DO-ES)...");
    
    // Write in batches để tránh timeout
    // Ghi từ hàng 2 trở đi (bỏ qua hàng 1 header)
    const BATCH_SIZE = 100;
    let batchCount = 0;
    let successCount = 0;
    let errorCount = 0;
    
    for (let startRow = 0; startRow < dataRowCount; startRow += BATCH_SIZE) {
      const endRow = Math.min(startRow + BATCH_SIZE, dataRowCount);
      const batchRows = endRow - startRow;
      const batchData = [];
      
      for (let r = startRow; r < endRow; r++) {
        batchData.push(onlineBlock[r] || []);
      }
      
      try {
        // Ghi vào sheet từ hàng 2 (startRow + 2), vì startRow=0 tương ứng với hàng 2 trong sheet
        const sheetRow = startRow + 2; // startRow=0 -> hàng 2, startRow=1 -> hàng 3, ...
        const batchRange = masterSh.getRange(sheetRow, ONLINE_START_COL, batchRows, dayColsCount);
        batchRange.setValues(batchData);
        SpreadsheetApp.flush();
        
        batchCount++;
        successCount += batchRows;
        Logger.log(`   ✓ Batch ${batchCount}: rows ${sheetRow}-${sheetRow + batchRows - 1} (${batchRows} rows)`);
        
        if (batchCount % 5 === 0) {
          Utilities.sleep(100);
        }
      } catch (batchError) {
        errorCount++;
        Logger.log(`   ✗ ERROR in batch ${batchCount} (rows ${startRow + 2}-${startRow + batchRows + 1}): ${batchError.message}`);
      }
    }
    
    Logger.log(`6) Write completed: ${batchCount} batches, ${successCount} rows written, ${errorCount} errors`);
    SpreadsheetApp.flush();
    Utilities.sleep(200);
  } else {
    Logger.log("5) No data to write");
  }
  
  // Toast notification
  try {
    const message = `Đã cập nhật ${updatedCells} ô CA ONLINE vào cột DO-ES` +
      (notFound.length ? ` (${notFound.length} mã không tìm thấy)` : "") +
      (errorCount > 0 ? ` (${errorCount} batch lỗi)` : "");
    masterSh.getRange(1, 1).setValue(masterSh.getRange(1, 1).getValue());
    SpreadsheetApp.getActiveSpreadsheet().toast(message, "Hoàn thành", 5);
    Logger.log(`Toast notification: ${message}`);
  } catch (e) {
    Logger.log(`Notification skipped. Finished: Updated ${updatedCells} ô.`);
  }
}

/**
 * IMPORT GOOGLE FORM CHẤM CÔNG OFFLINE -> SHEET TỔNG (Cột CB -> DF)
 * Xử lý CA OFFLINE: fill vào cột CB -> DF. Quy tắc 24h giống ONL: ngày từ cột G, chỉ ghi khi timestamp (A) cùng ngày với G.
 */
function importOfflineFormToMaster() {
  // ====== CONFIG ======
  const FORM_FILE_ID = "1GATYUk6jMyNIRyDI1FxH-I9ix6NafyE3PwJn2ptHJ1k"; // File chứa form responses
  const FORM_SHEET_NAME = "Form Responses 1";
  
  const MASTER_FILE_ID = "1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4";
  const MASTER_SHEET_NAME = "Chấm công th12/2025";
  
  const MASTER_EMP_COL = 2;     // cột mã nhân viên (B)
  const MASTER_HEADER_ROW = 1;  // hàng chứa số ngày 1..31
  const OFFLINE_START_COL = 80;  // Cột CB (80 = CB)
  const OFFLINE_END_COL = 110;   // Cột DF (110 = DF) = 31 cột
  
  // Mapping cột form: G = EM CHẤM CÔNG CHO NGÀY NÀO, H = CA LÀM VIỆC CỦA EM (0-based)
  const FORM_COL_TIMESTAMP = 0;      // A: Dấu thời gian
  const FORM_COL_EMAIL = 1;           // B: Email
  const FORM_COL_EMP_CODE = 2;        // C: Mã nhân viên
  const FORM_COL_NAME = 3;            // D: Họ và tên
  const FORM_COL_TEAM = 4;            // E: Team
  const FORM_COL_DATE = 6;            // G: EM CHẤM CÔNG CHO NGÀY NÀO
  const FORM_COL_SHIFT = 7;           // H: CA LÀM VIỆC CỦA EM
  const FORM_COL_PROOF = 8;           // I: Minh chứng
  const FORM_COL_TYPE = 9;            // J: EM CHẤM CÔNG CHO HÌNH THỨC (CA ONLINE / CA OFFLINE)
  const FORM_COL_WORK_TYPE = 8;       // I: Hình thức làm việc
  
  Logger.log("1) Opening form responses sheet...");
  const formSS = SpreadsheetApp.openById(FORM_FILE_ID);
  const formSh = formSS.getSheetByName(FORM_SHEET_NAME);
  if (!formSh) throw new Error("Không tìm thấy sheet form: " + FORM_SHEET_NAME);
  
  const formValues = formSh.getDataRange().getValues();
  Logger.log(`   Loaded ${formValues.length} rows from form`);
  
  // Debug: Log tất cả header để xem cấu trúc form
  if (formValues.length > 0) {
    const headerRow = formValues[0];
    Logger.log(`   Debug: All headers (${headerRow.length} columns):`);
    for (let c = 0; c < headerRow.length; c++) {
      Logger.log(`     Col ${c}: "${headerRow[c]}"`);
    }
  }
  
  // Tự động tìm cột TYPE từ header row (row 0)
  // Cột TYPE là cột J với header "EM CHẤM CÔNG CHO HÌNH THỨC:"
  let actualTypeCol = FORM_COL_TYPE;
  let foundTypeCol = false;
  if (formValues.length > 0) {
    const headerRow = formValues[0];
    for (let c = 0; c < headerRow.length; c++) {
      const headerText = String(headerRow[c] || "").toLowerCase().trim();
      // Tìm cột có chứa "em chấm công cho hình thức" hoặc "hình thức"
      if (headerText.includes("em chấm công cho hình thức") || 
          headerText.includes("chấm công cho hình thức") ||
          (headerText.includes("hình thức") && headerText.includes("chấm công"))) {
        actualTypeCol = c;
        foundTypeCol = true;
        Logger.log(`   Found TYPE column at index ${c} (header: "${headerRow[c]}")`);
        break;
      }
    }
    // Nếu không tìm thấy, thử tìm bằng "ca online" hoặc "ca offline" nhưng KHÔNG phải "minh chứng"
    if (!foundTypeCol) {
      for (let c = 0; c < headerRow.length; c++) {
        const headerText = String(headerRow[c] || "").toLowerCase().trim();
        if ((headerText.includes("ca online") || headerText.includes("ca offline")) &&
            !headerText.includes("minh chứng") && !headerText.includes("minh chung") &&
            !headerText.includes("proof") && !headerText.includes("drive")) {
          actualTypeCol = c;
          foundTypeCol = true;
          Logger.log(`   Found TYPE column at index ${c} (header: "${headerRow[c]}") by 'online/offline' keyword`);
          break;
        }
      }
    }
    if (!foundTypeCol) {
      Logger.log(`   WARNING: Could not find TYPE column automatically, using default index ${FORM_COL_TYPE} (column J)`);
    }
  }
  
  // Tự động tìm cột EMP_CODE từ header row
  let actualEmpCol = FORM_COL_EMP_CODE;
  if (formValues.length > 0) {
    const headerRow = formValues[0];
    for (let c = 0; c < headerRow.length; c++) {
      const headerText = String(headerRow[c] || "").toLowerCase().trim();
      if (headerText.includes("mã nhân viên") || headerText.includes("ma nhan vien") ||
          headerText.includes("employee") || headerText.includes("code")) {
        actualEmpCol = c;
        Logger.log(`   Found EMP_CODE column at index ${c} (header: "${headerRow[c]}")`);
        break;
      }
    }
  }
  
  // Tự động tìm cột SHIFT từ header row
  let actualShiftCol = FORM_COL_SHIFT;
  if (formValues.length > 0) {
    const headerRow = formValues[0];
    for (let c = 0; c < headerRow.length; c++) {
      const headerText = String(headerRow[c] || "").toLowerCase().trim();
      if (headerText.includes("ca làm việc") || headerText.includes("ca lam viec") ||
          headerText.includes("shift") || headerText.includes("ca sáng") || 
          headerText.includes("ca chiều") || headerText.includes("ca sang") ||
          headerText.includes("ca chieu")) {
        actualShiftCol = c;
        Logger.log(`   Found SHIFT column at index ${c} (header: "${headerRow[c]}")`);
        break;
      }
    }
  }
  
  // ====== 2) PARSE FORM DATA - CHỈ LẤY CA OFFLINE VÀ THÁNG 12/2025 ======
  Logger.log("2) Parsing form data (CA OFFLINE only, month 12/2025)...");
  // Map: empCode -> Map<dayStr, {morning: {in, out}, afternoon: {in, out}}>
  const offlineByEmpDay = new Map();
  // Mã nhân viên có thể là MHxxxx, LL082, HN045, etc. - không giới hạn format
  // Nới lỏng regex: cho phép chữ cái, số, và một số ký tự đặc biệt thường gặp
  // Loại bỏ các ký tự không hợp lệ như khoảng trắng, ký tự đặc biệt lạ
  const empRegex = /^[A-Z0-9_-]{2,}$/i;
  
  let skippedType = 0;
  let skippedEmp = 0;
  let skippedDate = 0;
  let skippedMonth = 0;
  let skippedTime = 0;
  let skippedCrossDay = 0;
  let processed = 0;
  const skippedEmpCodes = new Set(); // Để log các mã bị skip
  const validEmpCodes = new Set(); // Để log các mã hợp lệ
  const validButSkipped = []; // Để log các dòng có mã hợp lệ nhưng bị skip vì lý do khác
  
  // Debug: Log các giá trị unique trong cột TYPE để xem có gì
  const uniqueTypes = new Set();
  for (let r = 1; r < Math.min(100, formValues.length); r++) {
    const typeVal = String(formValues[r][actualTypeCol] || "").trim();
    if (typeVal) uniqueTypes.add(typeVal);
  }
  Logger.log(`   Debug: Found ${uniqueTypes.size} unique TYPE values in first 100 rows:`);
  Array.from(uniqueTypes).slice(0, 20).forEach((val, idx) => {
    Logger.log(`     ${idx + 1}. "${val}"`);
  });
  
  // Debug: Log vài giá trị type đầu tiên để kiểm tra
  Logger.log("   Debug: Checking first 10 rows for type values...");
  Logger.log(`   Using TYPE column index: ${actualTypeCol}, EMP_CODE column index: ${actualEmpCol}, SHIFT column index: ${actualShiftCol}`);
  for (let debugR = 1; debugR <= Math.min(10, formValues.length - 1); debugR++) {
    const debugRow = formValues[debugR];
    const debugType = String(debugRow[actualTypeCol] || "").trim();
    const debugEmp = String(debugRow[actualEmpCol] || "").trim();
    const debugShift = debugRow[actualShiftCol];
    const debugShiftStr = debugShift instanceof Date ? debugShift.toString() : String(debugShift || "").trim();
    // Log thêm các cột xung quanh để debug
    const debugTypePrev = String(debugRow[actualTypeCol - 1] || "").trim();
    const debugTypeNext = String(debugRow[actualTypeCol + 1] || "").trim();
    Logger.log(`   Row ${debugR + 1}: type[${actualTypeCol}]="${debugType}", emp="${debugEmp}", shift[${actualShiftCol}]="${debugShiftStr}"`);
  }
  
  // Bỏ qua header row (row 0)
  for (let r = 1; r < formValues.length; r++) {
    const row = formValues[r];
    
    // Chỉ xử lý CA OFFLINE - so sánh không phân biệt hoa thường và trim
    const type = String(row[actualTypeCol] || "").trim();
    const typeUpper = type.toUpperCase();
    // Cho phép các biến thể: "CA OFFLINE", "ca offline", "CA OFFLINE ", "CA OFFLINE - CƠ SỞ KHÁC", etc.
    // Kiểm tra nếu chứa "CA OFFLINE" (có thể có thêm text sau)
    if (!typeUpper.includes("CA OFFLINE")) {
      skippedType++;
      continue;
    }
    
    // Lấy mã nhân viên từ cột đã tìm được
    const empCodeRaw = String(row[actualEmpCol] || "").trim();
    if (!empCodeRaw) {
      skippedEmp++;
      skippedEmpCodes.add("(empty)");
      continue; // Bỏ qua nếu không có mã
    }
    if (!empRegex.test(empCodeRaw)) {
      skippedEmp++;
      skippedEmpCodes.add(empCodeRaw);
      continue; // Bỏ qua nếu mã không hợp lệ
    }
    const empCode = empCodeRaw.toUpperCase();
    validEmpCodes.add(empCode);
    
    // Lấy ngày ca làm việc từ cột G (EM CHẤM CÔNG CHO NGÀY NÀO). Quy tắc 24h: timestamp (A) phải cùng ngày → khác ngày thì bỏ.
    const dateValue = row[FORM_COL_DATE];
    let dayStr = null;
    let isDec2025 = false;
    let parsedYear = null;
    let parsedMonth = null;
    let parsedDay = null;
    
    if (dateValue instanceof Date) {
      parsedYear = dateValue.getFullYear();
      parsedMonth = dateValue.getMonth() + 1; // getMonth() trả về 0-11
      parsedDay = dateValue.getDate();
      if (parsedYear === 2025 && parsedMonth === 12) {
        isDec2025 = true;
        dayStr = String(parsedDay);
      }
    } else {
      const dateStr = String(dateValue || "").trim();
      // Parse từ string "DD/MM/YYYY" hoặc "D/M/YYYY" (có thể có thêm giờ "DD/MM/YYYY HH:MM:SS")
      // Regex sẽ match phần date trước dấu cách hoặc ký tự không phải số
      const dateMatch = dateStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s|$)/);
      if (dateMatch) {
        parsedDay = dateMatch[1];
        parsedMonth = dateMatch[2];
        parsedYear = dateMatch[3];
        // Kiểm tra tháng 12 (parseInt để xử lý cả "12" và "12")
        if (parsedYear === "2025" && parseInt(parsedMonth) === 12) {
          isDec2025 = true;
          dayStr = parsedDay;
        }
      } else {
        // Thử parse format khác: "YYYY-MM-DD" hoặc "DD-MM-YYYY" (có thể có thêm giờ)
        const dateMatch2 = dateStr.match(/(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})(?:\s|$)/);
        if (dateMatch2) {
          // Format YYYY-MM-DD
          parsedYear = dateMatch2[1];
          parsedMonth = dateMatch2[2];
          parsedDay = dateMatch2[3];
          if (parsedYear === "2025" && parseInt(parsedMonth) === 12) {
            isDec2025 = true;
            dayStr = parsedDay;
          }
        } else {
          // Thử format DD-MM-YYYY
          const dateMatch3 = dateStr.match(/(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})(?:\s|$)/);
          if (dateMatch3) {
            parsedDay = dateMatch3[1];
            parsedMonth = dateMatch3[2];
            parsedYear = dateMatch3[3];
            if (parsedYear === "2025" && parseInt(parsedMonth) === 12) {
              isDec2025 = true;
              dayStr = parsedDay;
            }
          }
        }
      }
    }
    
    // Debug: Log vài dòng đầu để xem parse ngày và shift type
    if (processed < 5 && empCode) {
      const debugShift = row[actualShiftCol];
      const debugShiftStr = debugShift instanceof Date ? debugShift.toString() : String(debugShift || "").trim();
      Logger.log(`   DEBUG Row ${r + 1}, emp=${empCode}: dateValue="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay}, isDec2025=${isDec2025}, shift[${actualShiftCol}]="${debugShiftStr}"`);
    }
    
    // Chỉ xử lý dữ liệu từ tháng 12/2025
    if (!isDec2025) {
      skippedMonth++;
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, date="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay} (not Dec 2025)`);
      }
      continue;
    }
    
    // Validate dayStr - chuyển sang số để so sánh chính xác và chuẩn hóa format
    const dayNum = parseInt(dayStr);
    if (!dayStr || isNaN(dayNum) || dayNum < 1 || dayNum > 31) {
      skippedDate++;
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, dayStr="${dayStr}" (invalid day)`);
      }
      continue;
    }
    // Chuẩn hóa dayStr: đảm bảo là string "1", "2", ..., "31" (không có leading zero)
    dayStr = String(dayNum);
    
    // Lấy loại ca và timestamp
    // Theo yêu cầu hiện tại: ưu tiên cột H (index 7) nếu chứa "check in/out"; fallback sang cột SHIFT đã detect; cuối cùng fallback sang cột G mặc định.
    const isShiftText_ = (v) => {
      if (!v) return false;
      if (v instanceof Date) return false;
      const s = String(v || '').toLowerCase();
      return s.includes('check in') || s.includes('check out') || s.includes('checkin') || s.includes('checkout');
    };
    let shiftTypeRaw = row[actualShiftCol];
    if (isShiftText_(row[7])) shiftTypeRaw = row[7]; // cột H (ưu tiên nếu đúng dữ liệu phân loại)
    else if (isShiftText_(row[actualShiftCol])) shiftTypeRaw = row[actualShiftCol];
    else if (isShiftText_(row[FORM_COL_SHIFT])) shiftTypeRaw = row[FORM_COL_SHIFT];

    if (shiftTypeRaw instanceof Date) {
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, day=${dayStr}: Shift value is Date object. Skipping.`);
      continue;
    }
    const shiftType = String(shiftTypeRaw || "").trim();

    const timestamp = row[FORM_COL_TIMESTAMP];

    // Quy tắc hiệu lực 24h (theo ngày chấm công): timestamp phải cùng ngày với dateValue đã parse.
    try {
      const tz = Session.getScriptTimeZone();
      const workDateObj = new Date(Number(parsedYear), Number(parsedMonth) - 1, Number(parsedDay), 12, 0, 0);
      const workKey = Utilities.formatDate(workDateObj, tz, 'yyyy-MM-dd');
      let tsKey = null;
      if (timestamp instanceof Date && !isNaN(timestamp.getTime())) {
        tsKey = Utilities.formatDate(timestamp, tz, 'yyyy-MM-dd');
      } else {
        const tsStr = String(timestamp || '').trim();
        const m1 = tsStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
        const m2 = !m1 ? tsStr.match(/(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})/) : null;
        if (m1) {
          const d = Number(m1[1]), mo = Number(m1[2]), y = Number(m1[3]);
          const dt = new Date(y, mo - 1, d, 12, 0, 0);
          tsKey = Utilities.formatDate(dt, tz, 'yyyy-MM-dd');
        } else if (m2) {
          const y = Number(m2[1]), mo = Number(m2[2]), d = Number(m2[3]);
          const dt = new Date(y, mo - 1, d, 12, 0, 0);
          tsKey = Utilities.formatDate(dt, tz, 'yyyy-MM-dd');
        }
      }
      if (!tsKey || tsKey !== workKey) {
        skippedCrossDay++;
        if (processed < 5 && empCode) {
          Logger.log(`   Skip cross-day entry Row ${r + 1}, emp=${empCode}, day=${dayStr}: workKey=${workKey}, tsKey=${tsKey}, shift="${shiftType}"`);
        }
        continue;
      }
    } catch (e) {
      skippedCrossDay++;
      Logger.log(`   Skip entry Row ${r + 1} due to cross-day check error: ${e && e.message ? e.message : e}`);
      continue;
    }
    
    // Parse timestamp để lấy giờ
    let timeStr = null;
    if (timestamp instanceof Date) {
      const hours = timestamp.getHours();
      const minutes = timestamp.getMinutes();
      timeStr = `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}`;
    } else {
      // Parse từ string "DD/MM/YYYY HH:MM:SS" hoặc "DD/MM/YYYY H:MM:SS"
      const timeMatch = String(timestamp || "").match(/(\d{1,2}):(\d{2}):\d{2}/);
      if (timeMatch) {
        const h = timeMatch[1];
        const m = timeMatch[2];
        timeStr = `${h.padStart(2, '0')}:${m}`;
      }
    }
    if (!timeStr) {
      skippedTime++;
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, day=${dayStr}, timestamp="${timestamp}" (cannot parse)`);
      }
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, day=${dayStr}: Cannot parse timestamp: ${timestamp}`);
      continue;
    }
    
    processed++;
    
    // Khởi tạo map nếu chưa có
    if (!offlineByEmpDay.has(empCode)) {
      offlineByEmpDay.set(empCode, new Map());
    }
    const dayMap = offlineByEmpDay.get(empCode);
    if (!dayMap.has(dayStr)) {
      dayMap.set(dayStr, { morning: { in: null, out: null }, afternoon: { in: null, out: null } });
    }
    const dayData = dayMap.get(dayStr);
    
    // Phân loại ca và gán vào đúng vị trí
    // Sử dụng toLowerCase() để match không phân biệt hoa thường
    const shiftLower = shiftType.toLowerCase();
    if (shiftLower.includes("check in ca sáng") || shiftLower.includes("check in ca sang")) {
      dayData.morning.in = timeStr;
    } else if (shiftLower.includes("check out ca sáng") || shiftLower.includes("check out ca sang")) {
      dayData.morning.out = timeStr;
    } else if (shiftLower.includes("check in ca chiều") || shiftLower.includes("check in ca chieu")) {
      dayData.afternoon.in = timeStr;
    } else if (shiftLower.includes("check out ca chiều") || shiftLower.includes("check out ca chieu")) {
      dayData.afternoon.out = timeStr;
    } else {
      // Log các loại ca không match để debug - log cả giá trị gốc để xem có phải Date object không
      const shiftTypeDebug = row[actualShiftCol];
      const shiftTypeDebugStr = shiftTypeDebug instanceof Date ? shiftTypeDebug.toString() : String(shiftTypeDebug || "").trim();
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, day=${dayStr}: Unknown shift type: "${shiftType}" (raw value: "${shiftTypeDebugStr}", column ${actualShiftCol})`);
    }
    // Bỏ qua "Check in ca tối" vì chỉ xử lý ca sáng và ca chiều
  }
  
  Logger.log(`   Parsed ${offlineByEmpDay.size} employees with offline check-in data`);
  Logger.log(`   Stats: processed=${processed}, skippedType=${skippedType}, skippedEmp=${skippedEmp}, skippedMonth=${skippedMonth}, skippedDate=${skippedDate}, skippedTime=${skippedTime}, skippedCrossDay=${skippedCrossDay}`);
  
  // Log các dòng có mã hợp lệ nhưng bị skip
  if (validButSkipped.length > 0) {
    Logger.log(`   Valid codes but skipped (samples):`);
    validButSkipped.forEach((msg, idx) => {
      Logger.log(`     ${idx + 1}. ${msg}`);
    });
  }
  
  // Debug: Log các mã hợp lệ và bị skip
  Logger.log(`   Valid employee codes found: ${validEmpCodes.size}`);
  if (validEmpCodes.size > 0 && validEmpCodes.size <= 50) {
    Logger.log(`   Valid codes: ${Array.from(validEmpCodes).join(", ")}`);
  } else if (validEmpCodes.size > 50) {
    Logger.log(`   Valid codes (first 50): ${Array.from(validEmpCodes).slice(0, 50).join(", ")}`);
  }
  
  Logger.log(`   Skipped employee codes: ${skippedEmpCodes.size}`);
  if (skippedEmpCodes.size > 0 && skippedEmpCodes.size <= 30) {
    Logger.log(`   Skipped codes (samples): ${Array.from(skippedEmpCodes).slice(0, 30).join(", ")}`);
  } else if (skippedEmpCodes.size > 30) {
    Logger.log(`   Skipped codes (first 30): ${Array.from(skippedEmpCodes).slice(0, 30).join(", ")}`);
  }
  
  // ====== 3) OPEN MASTER SHEET ======
  Logger.log("3) Opening master sheet...");
  const masterSS = SpreadsheetApp.openById(MASTER_FILE_ID);
  const masterSh = masterSS.getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) throw new Error("Không tìm thấy sheet tổng: " + MASTER_SHEET_NAME);
  
  const masterInfo = buildMasterInfo_(masterSh, MASTER_EMP_COL, MASTER_HEADER_ROW);
  const { empToRow, colByDay, minDayCol } = masterInfo;
  
  // Kiểm tra và đảm bảo mapping ngày -> cột đúng: ngày 1 -> CB (80), ngày 2 -> CC (81), ...
  const colOfDay1 = colByDay.get("1");
  if (colOfDay1 && colOfDay1 !== OFFLINE_START_COL) {
    Logger.log(`   WARNING: Day 1 is mapped to column ${colOfDay1}, but expected ${OFFLINE_START_COL} (CB). Adjusting mapping...`);
  }
  
  // ====== 4) BUILD DATA TO WRITE ======
  Logger.log("4) Building data to write (columns CB-DF)...");
  Logger.log(`   Mapping: Day 1 -> Column ${colOfDay1 || 'NOT FOUND'}, OFFLINE_START_COL=${OFFLINE_START_COL} (CB)`);
  const lastEmpRow = masterInfo.lastEmpRow;
  const dayColsCount = OFFLINE_END_COL - OFFLINE_START_COL + 1; // 31 cột
  
  // Khởi tạo mảng 2D: [row][col] = value
  // Chỉ khởi tạo cho các hàng dữ liệu (từ hàng 2 đến lastEmpRow), bỏ qua hàng 1 (header)
  // offlineBlock[0] tương ứng với hàng 2 trong sheet, offlineBlock[1] tương ứng với hàng 3, ...
  const dataRowCount = lastEmpRow - 1; // Số hàng dữ liệu (bỏ qua hàng header)
  const offlineBlock = [];
  for (let r = 0; r < dataRowCount; r++) {
    offlineBlock[r] = new Array(dayColsCount).fill("");
  }
  
  let updatedCells = 0;
  const notFound = [];
  
  for (const [empCode, dayMap] of offlineByEmpDay.entries()) {
    const row1 = empToRow.get(empCode);
    if (!row1) {
      notFound.push(empCode);
      continue;
    }
    // Bỏ qua hàng 1 (header) - chỉ xử lý từ hàng 2 trở đi
    if (row1 === 1) {
      Logger.log(`   WARNING: Skipping row 1 (header) for emp ${empCode}`);
      continue;
    }
    // row1 là hàng trong sheet (2, 3, 4, ...), chuyển sang index trong offlineBlock (0, 1, 2, ...)
    const r0 = row1 - 2; // Hàng 2 -> index 0, hàng 3 -> index 1, ...
    
    for (const [dayStr, dayData] of dayMap.entries()) {
      const dayNum = parseInt(dayStr);
      if (isNaN(dayNum) || dayNum < 1 || dayNum > 31) {
        Logger.log(`   WARNING: Invalid dayStr "${dayStr}"`);
        continue;
      }
      
      // Tính cột đích trực tiếp: ngày 1 -> CB (80), ngày 2 -> CC (81), ..., ngày 31 -> DF (110)
      // OFFLINE_START_COL = 80 (CB), ngày 1 -> offset 0 -> cột 80, ngày 2 -> offset 1 -> cột 81, ...
      const offlineC0 = dayNum - 1; // 0-based: ngày 1 -> 0, ngày 2 -> 1, ..., ngày 31 -> 30
      
      // Validate: offset phải từ 0 đến 30 (31 cột: CB=0, CC=1, ..., DF=30)
      if (offlineC0 < 0 || offlineC0 >= dayColsCount) {
        Logger.log(`   WARNING: Day ${dayStr} (dayNum=${dayNum}) offset ${offlineC0} is outside range [0, ${dayColsCount-1}]`);
        continue;
      }
      
      // Validate: r0 phải trong phạm vi offlineBlock
      if (r0 < 0 || r0 >= dataRowCount) {
        Logger.log(`   WARNING: Row index ${r0} (row ${row1}) is outside range [0, ${dataRowCount-1}]`);
        continue;
      }
      
      // Tính cột thực tế trong sheet để ghi
      const targetCol = OFFLINE_START_COL + offlineC0; // Ngày 1 -> 80, ngày 2 -> 81, ..., ngày 31 -> 110
      
      // Tạo format text dựa trên dữ liệu
      const parts = [];
      const hasMorning = dayData.morning.in || dayData.morning.out;
      const hasAfternoon = dayData.afternoon.in || dayData.afternoon.out;
      
      if (hasMorning && hasAfternoon) {
        // Có cả 2 ca -> "off 2 ca"
        const morningTimes = [];
        if (dayData.morning.in) morningTimes.push(dayData.morning.in);
        if (dayData.morning.out) morningTimes.push(dayData.morning.out);
        const afternoonTimes = [];
        if (dayData.afternoon.in) afternoonTimes.push(dayData.afternoon.in);
        if (dayData.afternoon.out) afternoonTimes.push(dayData.afternoon.out);
        
        const allTimes = [...morningTimes, ...afternoonTimes];
        offlineBlock[r0][offlineC0] = allTimes.join("\n") + "\noff 2 ca";
      } else if (hasMorning) {
        // Chỉ có ca sáng
        const morningTimes = [];
        if (dayData.morning.in) morningTimes.push(dayData.morning.in);
        if (dayData.morning.out) morningTimes.push(dayData.morning.out);
        offlineBlock[r0][offlineC0] = morningTimes.join("\n") + "\noff ca sáng";
      } else if (hasAfternoon) {
        // Chỉ có ca chiều
        const afternoonTimes = [];
        if (dayData.afternoon.in) afternoonTimes.push(dayData.afternoon.in);
        if (dayData.afternoon.out) afternoonTimes.push(dayData.afternoon.out);
        offlineBlock[r0][offlineC0] = afternoonTimes.join("\n") + "\noff ca chiều";
      }
      
      if (offlineBlock[r0][offlineC0]) {
        updatedCells++;
        // Debug: Log vài cell đầu để kiểm tra mapping
        if (updatedCells <= 5) {
          Logger.log(`   DEBUG: Writing day ${dayStr} (col ${targetCol}, offset ${offlineC0}) for emp ${empCode} at row ${row1}`);
        }
      }
    }
  }
  
  Logger.log(`   Prepared ${updatedCells} cells to update`);
  if (notFound.length) {
    Logger.log(`   Không tìm thấy ${notFound.length} mã trong sheet tổng (samples): ${notFound.slice(0, 20).join(", ")}`);
    if (notFound.length > 20) {
      Logger.log(`   ... và ${notFound.length - 20} mã khác`);
    }
  }
  
  // ====== 5) WRITE TO MASTER SHEET ======
  if (updatedCells > 0) {
    Logger.log("5) Writing to master sheet (columns CB-DF)...");
    
    // Write in batches để tránh timeout
    // Ghi từ hàng 2 trở đi (bỏ qua hàng 1 header)
    const BATCH_SIZE = 100;
    let batchCount = 0;
    let successCount = 0;
    let errorCount = 0;
    
    for (let startRow = 0; startRow < dataRowCount; startRow += BATCH_SIZE) {
      const endRow = Math.min(startRow + BATCH_SIZE, dataRowCount);
      const batchRows = endRow - startRow;
      const batchData = [];
      
      for (let r = startRow; r < endRow; r++) {
        batchData.push(offlineBlock[r] || []);
      }
      
      try {
        // Ghi vào sheet từ hàng 2 (startRow + 2), vì startRow=0 tương ứng với hàng 2 trong sheet
        const sheetRow = startRow + 2; // startRow=0 -> hàng 2, startRow=1 -> hàng 3, ...
        const batchRange = masterSh.getRange(sheetRow, OFFLINE_START_COL, batchRows, dayColsCount);
        batchRange.setValues(batchData);
        SpreadsheetApp.flush();
        
        batchCount++;
        successCount += batchRows;
        Logger.log(`   ✓ Batch ${batchCount}: rows ${sheetRow}-${sheetRow + batchRows - 1} (${batchRows} rows)`);
        
        if (batchCount % 5 === 0) {
          Utilities.sleep(100);
        }
      } catch (batchError) {
        errorCount++;
        Logger.log(`   ✗ ERROR in batch ${batchCount} (rows ${startRow + 2}-${startRow + batchRows + 1}): ${batchError.message}`);
      }
    }
    
    Logger.log(`6) Write completed: ${batchCount} batches, ${successCount} rows written, ${errorCount} errors`);
    SpreadsheetApp.flush();
    Utilities.sleep(200);
  } else {
    Logger.log("5) No data to write");
  }
  
  // Toast notification
  try {
    const message = `Đã cập nhật ${updatedCells} ô CA OFFLINE vào cột CB-DF` +
      (notFound.length ? ` (${notFound.length} mã không tìm thấy)` : "") +
      (errorCount > 0 ? ` (${errorCount} batch lỗi)` : "");
    masterSh.getRange(1, 1).setValue(masterSh.getRange(1, 1).getValue());
    SpreadsheetApp.getActiveSpreadsheet().toast(message, "Hoàn thành", 5);
    Logger.log(`Toast notification: ${message}`);
  } catch (e) {
    Logger.log(`Notification skipped. Finished: Updated ${updatedCells} ô.`);
  }
}

// ==================== MENU BUTTONS (GOOGLE SHEET UI) ====================

/**
 * Tạo menu khi mở Google Sheet.
 * LƯU Ý: Menu chỉ hiện khi script gắn với Spreadsheet và user có quyền chạy.
 */
function onOpen(e) {
  try {
    buildChamCongMenu_();
  } catch (err) {
    // Không throw để tránh làm lỗi onOpen
    Logger.log('onOpen error: ' + (err && err.message ? err.message : err));
  }
}

function buildChamCongMenu_() {
  const ui = SpreadsheetApp.getUi();
  const menu = ui.createMenu('CHẤM CÔNG');

  menu.addSubMenu(
    ui.createMenu('Vân tay (OFF)')
      .addItem('Import data vân tay', 'UI_importVanTay')
      .addSeparator()
      .addItem('Xử lí quên check in/out (OFF vân tay)', 'UI_offVanTayMissing')
      .addItem('Xử lí trễ (OFF vân tay)', 'UI_offVanTayLate')
      .addSeparator()
      .addItem('Tổng ca tháng (BU)', 'UI_totalCaThang')
      .addSeparator()
      .addItem('Highlight Lỗi trễ và quên check in out tự động', 'UI_highlightError')
  );

  menu.addSubMenu(
    ui.createMenu('ONL (Form)')
      .addItem('Import data chấm công ONL form (DO-ES)', 'UI_importOnlForm')
      .addSeparator()
      .addItem('Xử lí quên check in/out (ONL form)', 'UI_onlMissing')
      .addItem('Xử lí trễ (ONL form)', 'UI_onlLate')
  );

  menu.addSubMenu(
    ui.createMenu('OFF ngoài (Form)')
      .addItem('Import data chấm công OFF ngoài form (CB-DF)', 'UI_importOffNgoaiForm')
      .addSeparator()
      .addItem('Xử lí quên check in/out (OFF ngoài)', 'UI_offNgoaiMissing')
      .addItem('Xử lí trễ (OFF ngoài)', 'UI_offNgoaiLate')
  );

  menu.addToUi();
}

function UI_confirmRun_(title, message) {
  const ui = SpreadsheetApp.getUi();
  const res = ui.alert(title, message + '\n\nBấm OK để chạy, Cancel để huỷ.', ui.ButtonSet.OK_CANCEL);
  return res === ui.Button.OK;
}

function UI_run_(title, fn) {
  const ui = SpreadsheetApp.getUi();
  try {
    const out = fn();
    SpreadsheetApp.getActiveSpreadsheet().toast(`${title}: OK`, 'Hoàn thành', 5);
    return out;
  } catch (e) {
    const msg = (e && e.message) ? e.message : String(e);
    ui.alert(`${title}: LỖI`, msg, ui.ButtonSet.OK);
    throw e;
  }
}

// -------- Vân tay (OFF) --------
function UI_importVanTay() {
  if (!UI_confirmRun_('Import data vân tay', 'Sẽ import rawlog vân tay (nhiều cơ sở) vào sheet tổng.')) return;
  return UI_run_('Import data vân tay', () => importAllBranchesRawLogToMaster());
}

function UI_offVanTayMissing() {
  if (!UI_confirmRun_('OFF vân tay - Quên check in/out', 'Sẽ xử lí QUÊN check in/out từ dữ liệu vân tay (OFF).')) return;
  return UI_run_('OFF vân tay - Quên check in/out', () => applyAttendanceMissingOnly({ dryRun: false }));
}

function UI_offVanTayLate() {
  if (!UI_confirmRun_('OFF vân tay - Trễ', 'Sẽ xử lí TRỄ từ dữ liệu vân tay (OFF).')) return;
  return UI_run_('OFF vân tay - Trễ', () => applyAttendanceLateOnly({ dryRun: false }));
}

function UI_totalCaThang() {
  if (!UI_confirmRun_('Tổng ca tháng (BU)', 'Sẽ cập nhật cột BU = tổng ca off vân tay theo tháng hiện tại của sheet.')) return;
  return UI_run_('Tổng ca tháng (BU)', () => updateTongCaOffVanTayCommit());
}

function UI_highlightError() {
  if (!UI_confirmRun_('Highlight Lỗi trễ và quên check in out tự động', 'Sẽ highlight lỗi trễ và quên check in out tự động trong sheet tổng.')) return;
  return UI_run_('Highlight Lỗi trễ và quên check in out tự động', () => highlightProblematicCells());
}

// -------- ONL (Form) --------
function UI_importOnlForm() {
  if (!UI_confirmRun_('Import ONL form', 'Sẽ import dữ liệu chấm công ONL từ form vào cột DO-ES.')) return;
  return UI_run_('Import ONL form', () => importOnlineFormToMaster());
}

function UI_onlMissing() {
  if (!UI_confirmRun_('ONL - Quên check in/out', 'Sẽ xử lí QUÊN check in/out (và trễ >= 30 quy về quên) từ dữ liệu ONL.')) return;
  return UI_run_('ONL - Quên check in/out', () => applyOnlAttendanceMissingOnly({ dryRun: false }));
}

function UI_onlLate() {
  if (!UI_confirmRun_('ONL - Trễ', 'Sẽ xử lí TRỄ (<= 30 phút) từ dữ liệu ONL.')) return;
  return UI_run_('ONL - Trễ', () => applyOnlAttendanceLateOnly({ dryRun: false }));
}

// -------- OFF ngoài (Form) --------
function UI_importOffNgoaiForm() {
  if (!UI_confirmRun_('Import OFF ngoài form', 'Sẽ import dữ liệu chấm công OFF ngoài từ form vào cột CB-DF.')) return;
  return UI_run_('Import OFF ngoài form', () => importOfflineFormToMaster());
}

function UI_offNgoaiMissing() {
  if (!UI_confirmRun_('OFF ngoài - Quên check in/out', 'Sẽ xử lí QUÊN check in/out (và trễ >= 30 quy về quên) từ dữ liệu OFF ngoài.')) return;
  return UI_run_('OFF ngoài - Quên check in/out', () => applyOffAttendanceMissingOnly({ dryRun: false }));
}

function UI_offNgoaiLate() {
  if (!UI_confirmRun_('OFF ngoài - Trễ', 'Sẽ xử lí TRỄ (<= 30 phút) từ dữ liệu OFF ngoài.')) return;
  return UI_run_('OFF ngoài - Trễ', () => applyOffAttendanceLateOnly({ dryRun: false }));
}