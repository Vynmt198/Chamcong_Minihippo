/**
 * IMPORT RAWLOG (nhiều cơ sở) -> SHEET TỔNG (CHẤM CÔNG 2025)
 * Rawlog dạng block: mỗi nhân viên 1 block, header ngày 1..31, mỗi ô chứa nhiều giờ xuống dòng
 */
function importAllBranchesRawLogToMaster() {
  // ====== CONFIG ======
  const RAW_FILE_ID = "1ed1IK4X1bQxhBoz4tjUKEypIv6cipNKsUCcXPKjqy8o";
  // Test 1 cơ sở trước:
  const RAW_SHEETS = ["L4_HH"];
  // Khi OK thì bật đủ:
  // const RAW_SHEETS = ["L4_HH", "L1_HH", "L5_HH", "HDK", "TP"];

  const MASTER_FILE_ID = "1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4";  
  const MASTER_SHEET_NAME = "Chấm công th12/2025";

  const MASTER_EMP_COL = 2;     // cột mã nhân viên (B)
  const MASTER_HEADER_ROW = 1;  // hàng chứa số ngày 1..31

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

  // ====== 5) BUILD colByDay + find day range ======
  const lastCol = masterSh.getLastColumn();
  const header = masterSh.getRange(MASTER_HEADER_ROW, 1, 1, lastCol).getValues()[0];

  const colByDay = new Map(); // dayStr -> col (1-based)
  let minDayCol = null, maxDayCol = null;

  for (let c = 0; c < header.length; c++) {
    const day = parseDayFromValue_(header[c]);
    if (day) {
      const col1 = c + 1;
      colByDay.set(day, col1);
      if (minDayCol === null || col1 < minDayCol) minDayCol = col1;
      if (maxDayCol === null || col1 > maxDayCol) maxDayCol = col1;
    }
  }

  Logger.log("colByDay size=" + colByDay.size + ", minDayCol=" + minDayCol + ", maxDayCol=" + maxDayCol);

  if (minDayCol === null) throw new Error("Không tìm thấy header ngày 1..31 trong sheet tổng.");

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
function buildMasterInfo_(masterSh, masterEmpCol, masterHeaderRow) {
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
    const day = parseDayFromValue_(header[c]);
    if (day) {
      const col1 = c + 1;
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
    // PART: vẫn là nhân viên thường, ca sáng phải tính từ 08:30 như default
    template: { morningStart: "08:30", morningEnd: "12:00", afternoonStart: "13:30", afternoonEnd: "16:30" }
  },
  online: {
    template: { morningStart: "08:30", morningEnd: "12:00", afternoonStart: "13:15", afternoonEnd: "16:15" }
  }
};

function getEmployeeSchedule_(emp, baseCfg, role) {
  // baseCfg contains default fields
  const empId = String(emp || "").toUpperCase();
  const roleStr = String(role || "").trim().toUpperCase();

  // Role-based override (column F / FULL/PART)
  if (roleStr) {
    if (roleStr.includes('QL') || roleStr.includes('QUAN') || roleStr.includes('MANAGER')) {
      const t = SPECIAL_SCHEDULES.managers.template;
      return {
        useHalfDaySplit: true,
        morning: { start: t.morningStart, end: t.morningEnd },
        afternoon: { start: t.afternoonStart, end: t.afternoonEnd },
        cutoff: baseCfg.cutoff || SPECIAL_SCHEDULES.default.cutoff,
        lateThreshold: baseCfg.lateThreshold || SPECIAL_SCHEDULES.default.lateThreshold
      };
    }

    if (roleStr.includes('PART')) {
      const t = SPECIAL_SCHEDULES.parttime.template || SPECIAL_SCHEDULES.default;
      return {
        useHalfDaySplit: true,
        morning: { start: t.morningStart || SPECIAL_SCHEDULES.default.morningStart, end: t.morningEnd || SPECIAL_SCHEDULES.default.morningEnd },
        afternoon: { start: t.afternoonStart || SPECIAL_SCHEDULES.default.afternoonStart, end: t.afternoonEnd || SPECIAL_SCHEDULES.default.afternoonEnd },
        cutoff: baseCfg.cutoff || SPECIAL_SCHEDULES.default.cutoff,
        lateThreshold: baseCfg.lateThreshold || SPECIAL_SCHEDULES.default.lateThreshold
      };
    }

    if (roleStr.includes('ONL') || roleStr.includes('ONLINE')) {
      const t = SPECIAL_SCHEDULES.online.template || SPECIAL_SCHEDULES.default;
      return {
        useHalfDaySplit: true,
        morning: { start: t.morningStart || SPECIAL_SCHEDULES.default.morningStart, end: t.morningEnd || SPECIAL_SCHEDULES.default.morningEnd },
        afternoon: { start: t.afternoonStart || SPECIAL_SCHEDULES.default.afternoonStart, end: t.afternoonEnd || SPECIAL_SCHEDULES.default.afternoonEnd },
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
      afternoon: { start: t.afternoonStart, end: t.afternoonEnd },
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
    afternoon: { start: baseCfg.afternoonStart || SPECIAL_SCHEDULES.default.afternoonStart, end: baseCfg.afternoonEnd || SPECIAL_SCHEDULES.default.afternoonEnd },
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
  
  const morningStartMin = timeStrToMinutes_(schedule.morning.start);
  const morningEndMin = timeStrToMinutes_(schedule.morning.end);
  const afternoonStartMin = timeStrToMinutes_(schedule.afternoon.start);
  const afternoonEndMin = timeStrToMinutes_(schedule.afternoon.end);
  
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
    missingIn: morningIn === null,
    missingOut: morningIn !== null && morningOut === null,
    lateMinutes: morningLateMinutes,
    earlyMinutes: 0
  };

  out['afternoon'] = {
    times: afternoonTimes,
    in: afternoonIn,
    out: afternoonOut,
    missingIn: afternoonIn === null,
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
 */
function handleMissingCheckInOutSimple_(session, human, dayStr, month) {
  const notes = [];
  let offForgotDelta = 0;

  // CHỈ báo lỗi khi session thực sự có làm (có ít nhất 1 mốc thời gian hợp lệ)
  // Nếu session không có mốc thời gian nào (times.length === 0), coi như nhân viên không làm ca đó, không báo lỗi
  const hasTimes = session && Array.isArray(session.times) && session.times.length > 0;
  if (!hasTimes) {
    return { notes, offForgotDelta }; // Không có mốc thời gian → không làm ca này → không báo lỗi
  }

  // Phát hiện lỗi quên check-in (chỉ khi có mốc thời gian nhưng thiếu check-in)
  if (session.missingIn && session.in === null) {
    notes.push(`- Quên check in ${human} ${dayStr}/${month}`);
    offForgotDelta++;
  }
  // Phát hiện lỗi quên check-out (chỉ khi đã có check-in nhưng thiếu check-out)
  else if (session.missingOut && session.in !== null && session.out === null) {
    notes.push(`- Quên check out ${human} ${dayStr}/${month}`);
    offForgotDelta++;
  }

  return { notes, offForgotDelta };
}

/**
 * Helper: phát hiện lỗi TRỄ check-in cho 1 session (simple mode)
 * - session.lateMinutes đã là số phút trễ so với giờ bắt đầu ca
 * - >=30 phút: ghi "trễ trên 30 phút" và +1 vào lateCount, đồng thời đẩy vào problematicCells
 * - <30 phút: ghi "trễ dưới 30 phút", không cộng vào lateCount
 * Trả về: { notes: string[], lateDelta: number }
 */
function handleLateSimple_(session, human, dayStr, month, masterInfo, r0, emp, sessionName, problematicCells) {
  const notes = [];
  let lateDelta = 0;

  if (session.in && session.lateMinutes && session.lateMinutes > 0) {
    const lateMinutes = Math.round(session.lateMinutes);

    if (session.lateMinutes >= 30) {
      // Trễ trên 30 phút - ghi rõ và đếm vào lateCount
      notes.push(`- Check in trễ trên 30 phút (${lateMinutes} phút) ${human} ${dayStr}/${month}`);
      lateDelta++;

      // Thêm vào problematicCells để highlight
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
    } else {
      // Trễ dưới 30 phút - ghi rõ nhưng không đếm vào lateCount
      notes.push(`- Check in trễ dưới 30 phút (${lateMinutes} phút) ${human} ${dayStr}/${month}`);
    }
  }

  return { notes, lateDelta };
}

/**
 * Tính số ca off vân tay cho một nhân viên trong một khoảng ngày cụ thể (tuần)
 * Logic: 
 * - Nếu có check in nhưng quên check out = 1 ca off
 * - Nếu có check out nhưng quên check in = 1 ca off  
 * - Nếu có cả check in và check out = không tính (ca thành công)
 * - Nếu không có gì = không tính
 * @param {number} r0 - Row index (0-based) của nhân viên trong master sheet
 * @param {number} startDay - Ngày bắt đầu tuần (1-31)
 * @param {number} endDay - Ngày kết thúc tuần (1-31)
 * @param {Object} masterInfo - Thông tin master sheet
 * @param {Object} schedule - Lịch làm việc của nhân viên
 * @param {Object} cfg - Config attendance
 * @return {number} - Tổng số ca off vân tay trong tuần
 */
function calculateOffVanTayCountForWeek_(r0, startDay, endDay, masterInfo, schedule, cfg) {
  let totalOffCount = 0;

  // Duyệt qua tất cả các ngày trong tuần
  for (let day = startDay; day <= endDay; day++) {
    const dayStr = String(day);
    const col1 = masterInfo.colByDay.get(dayStr);
    if (!col1) continue; // Ngày này không có trong header

    const c0 = col1 - masterInfo.minDayCol;
    if (c0 < 0 || c0 >= masterInfo.dayColsCount) continue;

    // Lấy dữ liệu từ dayBlock (đã merge từ raw và master)
    const existing = masterInfo.dayBlock[r0][c0];
    let times = extractTimesFromCell_(existing);
    if (!times || !times.length) times = extractTimes_(String(existing || ''));

    if (!times || times.length === 0) continue; // Không có dữ liệu = không tính

    const sessionsMap = computeSessionsBySchedule_(times, schedule, null);

    // Đếm số ca có vấn đề (missingIn hoặc missingOut)
    for (const [sessionName, session] of Object.entries(sessionsMap)) {
      // Chỉ tính nếu có ít nhất 1 trong 2 (check in hoặc check out) nhưng thiếu cái kia
      if (session.missingIn && !session.missingOut) {
        // Có check out nhưng quên check in = 1 ca off
        totalOffCount++;
      } else if (session.missingOut && !session.missingIn) {
        // Có check in nhưng quên check out = 1 ca off
        totalOffCount++;
      }
      // Nếu cả 2 đều có (missingIn=false, missingOut=false) = ca thành công, không tính
      // Nếu cả 2 đều thiếu (missingIn=true, missingOut=true) = không có dữ liệu, không tính
    }
  }

  return totalOffCount;
}

function findHeaderCols_(headerRow) {
  const map = {};
  const norm = (s) => normalize_(s || "");
  const headers = headerRow.map(h => ({ raw: h, n: norm(h) }));

  // detail columns (chi tiet) -> collect in order
  const detailIdx = [];
  headers.forEach((h, idx) => { if (h.n.includes("chi tiet")) detailIdx.push(idx + 1); });
  if (detailIdx.length) map.detail2Col = detailIdx[0];
  if (detailIdx.length > 1) map.detail3Col = detailIdx[1];
  // lateNoteCol: ưu tiên Chi tiết(2) (thường là cột S) để ghi note TRỄ
  map.lateNoteCol = map.detail2Col || null;

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

  // vân tay tuần columns (BP-BT) - tìm các cột có "van tay tuan" và parse khoảng ngày
  map.vanTayTuanCols = [];
  headers.forEach((h, idx) => {
    if (h.n.includes("van tay tuan") || h.n.includes("vân tay tuần")) {
      // Parse khoảng ngày từ tiêu đề (ví dụ: "1-7", "8-14", "15-21", "22-28", "29-31")
      const dayRangeMatch = h.n.match(/(\d{1,2})\s*-\s*(\d{1,2})/);
      if (dayRangeMatch) {
        const startDay = Number(dayRangeMatch[1]);
        const endDay = Number(dayRangeMatch[2]);
        map.vanTayTuanCols.push({
          col: idx + 1,
          startDay: startDay,
          endDay: endDay,
          headerText: h.raw
        });
      }
    }
  });
  // Sắp xếp theo startDay để đảm bảo thứ tự
  map.vanTayTuanCols.sort((a, b) => a.startDay - b.startDay);

  // totals and flags
  headers.forEach((h, idx) => {
    const i = idx + 1;
    if (!map.totalLateCol && h.n.includes("tong tre")) map.totalLateCol = i;
    if (!map.offForgotCol && h.n.includes("off quen")) map.offForgotCol = i;
    // Bỏ tìm cột V (onlForgotCol) - không sử dụng nữa
    // if (!map.onlForgotCol && h.n.includes("onl quen")) map.onlForgotCol = i;
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

  Logger.log("1) Loading data...");
  const timesByEmpDay = buildTimesFromRawSheets_(RAW_FILE_ID, RAW_SHEETS);
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
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

      // Lấy dữ liệu chấm công từ dayBlock hoặc raw map
      let timesArr = [];
      const rawDayMap = timesByEmpDay.get(emp);
      if (rawDayMap && rawDayMap.get(dayStr)) {
        timesArr = Array.from(rawDayMap.get(dayStr));
      } else {
        // Nếu không có trong raw map, lấy từ dayBlock
        const existing = masterInfo.dayBlock[r0][c0];
        const extracted = extractTimesFromCell_(existing);
        if (extracted && extracted.length) {
          timesArr = extracted;
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

  // Duyệt qua TẤT CẢ nhân viên có dữ liệu trong raw data (không cần schedule)
  for (const [emp, dayMap] of timesByEmpDay.entries()) {
    // YÊU CẦU: Bỏ qua hoàn toàn nhân viên MH0008 (không xử lý trễ, không quên in/out)
    const empId = String(emp || '').toUpperCase();
    if (empId === 'MH0008') continue;

    const r1 = masterInfo.empToRow.get(emp);
    if (!r1) continue; // employee not in master attendance sheet
    const r0 = r1 - 1;

    let notesForDetail = [];
    let lateCount = 0, offForgotCount = 0;

    // Lấy schedule template dựa trên role (chỉ để tính lateMinutes, không dùng để check đăng ký ca)
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(emp) : undefined;
    const scheduleTemplate = getEmployeeSchedule_(emp, cfg, role);

    // Duyệt qua TẤT CẢ các ngày có dữ liệu trong raw data
    for (const [dayStr, timesSet] of dayMap.entries()) {
      const timesArr = Array.from(timesSet);

      // Nếu không có times trong raw, thử lấy từ master sheet
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
        // Không có dữ liệu - bỏ qua (không tính vắng, không check schedule)
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

        // Với chế độ đơn giản (không có đăng ký ca), nếu session không có bất kỳ mốc giờ nào
        // thì coi như nhân sự KHÔNG LÀM ca đó, không ghi lỗi quên check in/out
        if (!session || !Array.isArray(session.times) || session.times.length === 0) {
          continue;
        }

        const human = humanForSession(sessionName);

        // 1) Xử lý QUÊN CHECK IN/OUT (tùy theo mode)
        if (runMissing) {
          const missingRes = handleMissingCheckInOutSimple_(session, human, dayStr, month);
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

  // Load master sheet info
  Logger.log('2) Loading master sheet info...');
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  const month = parseMonthFromSheetName_(MASTER_SHEET_NAME) || 12;
  const headerMap = findHeaderCols_(masterInfo.header);

  // Compute changes - chỉ dựa vào raw data, không cần schedule
  Logger.log('3) Analyzing attendance (simple mode - no schedule check)...');
  const mode = opts.mode || 'both'; // 'both' | 'late' | 'missing'
  const result = prepareAttendanceChangesSimple_(timesByEmpDay, masterInfo, cfg, month, mode);

  const changes = result.changes || new Map();
  const problematicCells = result.problematicCells || [];

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
    const lateNotes = allNotes.filter(n => typeof n === 'string' && n.toLowerCase().includes('trễ'));
    const missingNotes = allNotes.filter(n => typeof n === 'string' && n.toLowerCase().includes('quên check'));

    // Ghi note TRỄ vào cột S (lateNoteCol)
    if (lateNoteCol && lateNotes.length) {
      const prevLate = String(newLateNote[r0] || '').trim();
      newLateNote[r0] = (prevLate ? prevLate + '\n' : '') + lateNotes.join('\n');
    }
    // Ghi note QUÊN CHECK IN/OUT vào cột W (noteCol)
    if (noteCol && missingNotes.length) {
      const prev = String(newNote[r0] || '').trim();
      newNote[r0] = (prev ? prev + '\n' : '') + missingNotes.join('\n');
    }
    if (headerMap.totalLateCol) {
      const prev = Number(newTotalLate[r0] || 0);
      newTotalLate[r0] = prev + Number(v.lateCount || 0);
    }
    if (headerMap.offForgotCol) {
      const prev = Number(newOffForgot[r0] || 0);
      newOffForgot[r0] = prev + Number(v.offForgotCount || 0);
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
      const noteColToWrite = headerMap.noteCol || headerMap.detail2Col;
      if (noteColToWrite) {
        const prev = String(noteArr[r0] || '').trim();
        masterSh.getRange(rowNum, noteColToWrite).setValue((prev ? prev + '\n' : '') + v.notes.join('\n'));
      }
      if (headerMap.totalLateCol) masterSh.getRange(rowNum, headerMap.totalLateCol).setValue(Number(totalLateArr[r0] || 0) + Number(v.lateCount || 0));
      if (headerMap.offForgotCol) masterSh.getRange(rowNum, headerMap.offForgotCol).setValue(Number(offForgotArr[r0] || 0) + Number(v.offForgotCount || 0));
      // Bỏ xử lý onlForgotCol - không sử dụng nữa
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
 * Tính toán và cập nhật các cột BP-BT "Vân tay tuần" với số ca off vân tay theo từng tuần
 * Cột BU (TỔNG ca off vân tay) đã có hàm SUM tự động, không cần xử lý
 * @param {boolean} dryRun - Nếu true chỉ preview, không ghi vào sheet
 * @return {Object} - Kết quả với số nhân viên đã cập nhật
 */
function updateVanTayTuanColumns(dryRun = true) {
  const MASTER_FILE_ID = "1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4";
  const MASTER_SHEET_NAME = "Chấm công th12/2025";
  const cfg = { morningStart: "08:30", afternoonStart: "13:15", cutoff: "12:00", lateThreshold: 30, maxTimesThreshold: 4 };

  Logger.log("1) Opening master sheet...");
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) throw new Error("Không tìm thấy sheet tổng: " + MASTER_SHEET_NAME);

  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  const headerMap = findHeaderCols_(masterInfo.header);

  if (!headerMap.vanTayTuanCols || headerMap.vanTayTuanCols.length === 0) {
    throw new Error("Không tìm thấy các cột 'Vân tay tuần' (BP-BT) trong sheet tổng");
  }

  Logger.log("2) Found " + headerMap.vanTayTuanCols.length + " vân tay tuần columns:");
  headerMap.vanTayTuanCols.forEach(vt => {
    Logger.log(`   - Col ${vt.col}: ${vt.startDay}-${vt.endDay} (${vt.headerText})`);
  });

  Logger.log("3) Calculating off van tay counts for each week...");
  const lastEmpRow = masterInfo.lastEmpRow;
  const weekCounts = new Map(); // Map<r0, Map<weekCol, count>>

  // Duyệt qua tất cả nhân viên có trong master sheet
  for (const [emp, r1] of masterInfo.empToRow.entries()) {
    const r0 = r1 - 1;
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(emp) : undefined;
    const schedule = getEmployeeSchedule_(emp, cfg, role);

    const empWeekCounts = new Map();

    // Tính số ca off cho từng tuần
    for (const weekCol of headerMap.vanTayTuanCols) {
      const offCount = calculateOffVanTayCountForWeek_(
        r0,
        weekCol.startDay,
        weekCol.endDay,
        masterInfo,
        schedule,
        cfg
      );
      empWeekCounts.set(weekCol.col, offCount);
    }

    weekCounts.set(r0, empWeekCounts);
  }

  Logger.log("4) Calculated counts for " + weekCounts.size + " employees");

  if (dryRun) {
    Logger.log("PREVIEW (dryRun) - Sample counts:");
    let i = 0;
    for (const [r0, empWeekCounts] of weekCounts.entries()) {
      const emp = Array.from(masterInfo.empToRow.entries()).find(([e, r]) => r === r0 + 1);
      const countsStr = Array.from(empWeekCounts.entries())
        .map(([col, count]) => `Col ${col}: ${count}`)
        .join(", ");
      Logger.log(`  Row ${r0 + 1} (${emp ? emp[0] : '?'}): ${countsStr}`);
      if (++i >= 10) break;
    }
    return { employeesCount: weekCounts.size, dryRun: true };
  }

  // Cập nhật vào sheet
  Logger.log("5) Updating vân tay tuần columns...");

  // Chuẩn bị dữ liệu để ghi: Map<col, array of values>
  // Lưu ý: r0 là 0-based index, nhưng hàng trong sheet bắt đầu từ 1 (hàng 1 = header)
  // Dữ liệu nhân viên bắt đầu từ hàng 2, nên cần map r0 -> rowNum = r0 + 1
  // Nhưng khi ghi, chỉ ghi từ hàng 2 đến lastEmpRow (bỏ qua hàng 1 header)
  const dataRowCount = lastEmpRow - 1; // Số hàng dữ liệu (bỏ qua hàng header)
  const columnValues = {};
  for (const weekCol of headerMap.vanTayTuanCols) {
    // Khởi tạo mảng với giá trị 0 cho tất cả các hàng dữ liệu (từ hàng 2)
    columnValues[weekCol.col] = new Array(dataRowCount).fill(0);
  }

  // Cập nhật giá trị mới từ kết quả tính toán
  let updatedCount = 0;
  for (const [r0, empWeekCounts] of weekCounts.entries()) {
    const rowNum = r0 + 1; // rowNum trong sheet (1-based)
    if (rowNum > lastEmpRow) {
      Logger.log(`WARNING: Row ${rowNum} exceeds lastEmpRow ${lastEmpRow}, skipping`);
      continue;
    }
    if (rowNum === 1) {
      Logger.log(`WARNING: Row ${rowNum} is header row, skipping`);
      continue;
    }

    // Map rowNum (1-based) sang index trong mảng (0-based, bỏ qua hàng header)
    const dataIndex = rowNum - 2; // Hàng 2 -> index 0, hàng 3 -> index 1, ...

    for (const [col, count] of empWeekCounts.entries()) {
      if (columnValues[col] && dataIndex >= 0 && dataIndex < columnValues[col].length) {
        columnValues[col][dataIndex] = count;
        Logger.log(`  Row ${rowNum}, Col ${col}: ${count} ca off`);
      } else {
        Logger.log(`WARNING: Column ${col} or dataIndex ${dataIndex} invalid`);
      }
    }
    updatedCount++;
  }

  // Ghi tất cả các cột cùng lúc (bắt đầu từ hàng 2, bỏ qua hàng header)
  Logger.log("6) Writing to sheet (starting from row 2)...");
  let writeCount = 0;
  for (const weekCol of headerMap.vanTayTuanCols) {
    const values = columnValues[weekCol.col].map(x => [x || 0]);
    try {
      // Ghi từ hàng 2 đến lastEmpRow (bỏ qua hàng 1 header)
      const range = masterSh.getRange(2, weekCol.col, dataRowCount, 1);
      range.setValues(values);
      Logger.log(`  ✓ Wrote column ${weekCol.col} (${weekCol.startDay}-${weekCol.endDay}) - rows 2 to ${lastEmpRow}`);
      writeCount++;
    } catch (e) {
      Logger.log(`  ✗ ERROR writing column ${weekCol.col}: ${e.message}`);
    }
  }

  Logger.log("7) Updated " + updatedCount + " employees across " + writeCount + " week columns");

  try {
    const ui = SpreadsheetApp.getUi();
    ui.alert(`Hoàn thành! Đã cập nhật ${updatedCount} nhân viên trong ${writeCount} cột vân tay tuần.`);
  } catch (e) {
    Logger.log(`Alert skipped (no UI available). Updated ${updatedCount} employees in ${writeCount} columns.`);
  }

  return { employeesCount: updatedCount, weekColumnsCount: writeCount, dryRun: false };
}

/**
 * Helper function để ghi trực tiếp vào sheet (không preview)
 * Gọi hàm này để cập nhật các cột vân tay tuần
 */
function updateVanTayTuanColumnsCommit() {
  Logger.log('Running commit: updateVanTayTuanColumns(dryRun=false)');
  return updateVanTayTuanColumns(false);
}

/**
 * Tính số ca vắng OFF và danh sách ngày vắng cho một nhân viên dựa trên schedule và dữ liệu chấm công
 * Logic: Nếu có đăng ký ca trong schedule nhưng không có check-in/out trong ngày đó = 1 ca vắng
 * @param {number} r0 - Row index (0-based) của nhân viên trong master sheet
 * @param {string} emp - Mã nhân viên
 * @param {Object} masterInfo - Thông tin master sheet
 * @param {Map} scheduleMap - Map<empCode, Map<dayStr, Set<sessionName>>> từ loadScheduleRegistrations_
 * @param {Object} schedule - Lịch làm việc của nhân viên
 * @param {Object} cfg - Config attendance
 * @param {number} month - Tháng hiện tại
 * @return {Object} - {totalVangCount: number, vangDays: Array<{dayStr, sessionName}>}
 */
function calculateVangOffCount_(r0, emp, masterInfo, scheduleMap, schedule, cfg, month) {
  let totalVangCount = 0;
  const vangDays = []; // Array of {dayStr, sessionName}

  // Lấy schedule đăng ký của nhân viên này
  const empSchedule = scheduleMap.get(emp);
  if (!empSchedule || empSchedule.size === 0) {
    // Không có đăng ký ca nào = không có vắng
    return { totalVangCount: 0, vangDays: [] };
  }

  // Helper để chuyển session name sang tên hiển thị
  const humanForSession = (sess) => {
    if (!sess) return 'ca';
    if (sess === 'morning') return 'ca sáng';
    if (sess === 'afternoon') return 'ca chiều';
    if (sess === 'evening') return 'ca tối';
    return 'ca';
  };

  // Duyệt qua tất cả các ngày có đăng ký ca
  for (const [dayStr, registeredSessions] of empSchedule.entries()) {
    const col1 = masterInfo.colByDay.get(dayStr);
    if (!col1) continue; // Ngày này không có trong header

    const c0 = col1 - masterInfo.minDayCol;
    if (c0 < 0 || c0 >= masterInfo.dayColsCount) continue;

    // Lấy dữ liệu chấm công từ dayBlock
    const existing = masterInfo.dayBlock[r0][c0];
    let times = extractTimesFromCell_(existing);
    if (!times || !times.length) times = extractTimes_(String(existing || ''));

    // Nếu không có dữ liệu chấm công = tất cả các ca đăng ký đều vắng
    if (!times || times.length === 0) {
      for (const registeredSession of registeredSessions) {
        totalVangCount++;
        vangDays.push({ dayStr, sessionName: registeredSession });
        Logger.log(`DEBUG VẮNG: emp=${emp} day=${dayStr} session=${registeredSession} - không có dữ liệu chấm công`);
      }
      continue;
    }

    // Nếu có dữ liệu chấm công, tính sessions và so sánh
    const sessionsMap = computeSessionsBySchedule_(times, schedule, registeredSessions);

    // Kiểm tra từng ca đăng ký
    for (const registeredSession of registeredSessions) {
      // Map session name từ schedule (morning/afternoon/evening) sang session name trong sessionsMap
      let sessionFound = false;

      // Tìm session tương ứng trong sessionsMap
      for (const [sessionName, session] of Object.entries(sessionsMap)) {
        // So khớp session: morning -> morning, afternoon -> afternoon, evening -> evening
        const sessionNameLower = sessionName.toLowerCase();
        const registeredLower = String(registeredSession).toLowerCase();

        if (sessionNameLower === registeredLower ||
          (registeredLower === 'morning' && (sessionNameLower.includes('sang') || sessionNameLower.includes('morning'))) ||
          (registeredLower === 'afternoon' && (sessionNameLower.includes('chieu') || sessionNameLower.includes('afternoon'))) ||
          (registeredLower === 'evening' && (sessionNameLower.includes('toi') || sessionNameLower.includes('evening')))) {
          // Có session này trong chấm công
          // Nếu có check-in hoặc check-out = không vắng
          if (session.in || session.out) {
            sessionFound = true;
            break;
          }
        }
      }

      // Nếu không tìm thấy session hoặc không có check-in/out = vắng
      if (!sessionFound) {
        totalVangCount++;
        vangDays.push({ dayStr, sessionName: registeredSession });
        Logger.log(`DEBUG VẮNG: emp=${emp} day=${dayStr} session=${registeredSession} - không có check-in/out`);
      }
    }
  }

  return { totalVangCount, vangDays };
}

/**
 * Tính toán và cập nhật cột Y "VẮNG OFF" dựa trên schedule và dữ liệu chấm công
 * Logic: Nếu có đăng ký ca trong schedule nhưng không có check-in/out = 1 ca vắng
 * @param {boolean} dryRun - Nếu true chỉ preview, không ghi vào sheet
 * @return {Object} - Kết quả với số nhân viên đã cập nhật
 */
function updateVangOffColumn(dryRun = true) {
  const SCHEDULE_FILE_ID = '1oKFAsC-mhAtA_yzHk8TwC3k5cCYzdNKFTgYSxfbDsSo';
  const SCHEDULE_SHEETS = ['LỊCH LÀM T12/2025', 'PAGE LỄ TÂN - LỊCH LÀM 2025'];
  const MASTER_EMP_FILE_ID = '1_szrWl2X-6Kcp7lpdl4HmBo7uciLqDGO-VWq1uie3HY';
  const MASTER_EMP_SHEET = 'MÃ SỐ NHÂN VIÊN';
  const MASTER_FILE_ID = "1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4";
  const MASTER_SHEET_NAME = "Chấm công th12/2025";
  const cfg = { morningStart: "08:30", afternoonStart: "13:15", cutoff: "12:00", lateThreshold: 30, maxTimesThreshold: 4 };

  Logger.log("1) Loading schedule registrations (OFF only)...");
  const nameMap = buildNameToEmpMap_(MASTER_EMP_FILE_ID, MASTER_EMP_SHEET);
  // Chỉ load OFF để tính vắng OFF (không tính những người không đăng ký OFF)
  const scheduleMap = loadScheduleRegistrations_(SCHEDULE_FILE_ID, SCHEDULE_SHEETS, nameMap);
  Logger.log("   Loaded " + scheduleMap.size + " employees with OFF registrations");

  Logger.log("2) Opening master sheet...");
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) throw new Error("Không tìm thấy sheet tổng: " + MASTER_SHEET_NAME);

  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  const headerMap = findHeaderCols_(masterInfo.header);

  if (!headerMap.vangOffCol) {
    throw new Error("Không tìm thấy cột 'VẮNG OFF' (Y) trong sheet tổng");
  }

  if (!headerMap.detail4Col) {
    Logger.log("WARNING: Không tìm thấy cột 'Chi tiết(4)' (AA), sẽ thử dùng cột 27 (AA) trực tiếp");
    // Fallback: dùng cột 27 (AA) trực tiếp
    headerMap.detail4Col = 27;
  } else {
    Logger.log("Found detail4Col (AA) at column " + headerMap.detail4Col);
  }

  Logger.log("3) Calculating vắng OFF counts and notes...");
  const lastEmpRow = masterInfo.lastEmpRow;
  const month = parseMonthFromSheetName_(MASTER_SHEET_NAME) || 12;
  const vangOffData = new Map(); // r0 -> {count, notes}

  // Helper để chuyển session name sang tên hiển thị
  const humanForSession = (sess) => {
    if (!sess) return 'ca';
    if (sess === 'morning') return 'ca sáng';
    if (sess === 'afternoon') return 'ca chiều';
    if (sess === 'evening') return 'ca tối';
    return 'ca';
  };

  // Duyệt qua tất cả nhân viên có trong master sheet
  for (const [emp, r1] of masterInfo.empToRow.entries()) {
    const r0 = r1 - 1;
    const role = masterInfo.empToRole ? masterInfo.empToRole.get(emp) : undefined;
    const schedule = getEmployeeSchedule_(emp, cfg, role);

    // Tính số ca vắng OFF và danh sách ngày vắng
    const result = calculateVangOffCount_(r0, emp, masterInfo, scheduleMap, schedule, cfg, month);
    const vangCount = result.totalVangCount;
    const vangDays = result.vangDays;

    // Debug log
    if (vangCount > 0) {
      Logger.log(`DEBUG: emp=${emp} has ${vangCount} vắng, vangDays=${vangDays.length}`);
    }

    // Tạo notes cho cột AA
    const notes = [];
    if (vangDays.length > 0) {
      // Nhóm theo ngày để tạo note
      const daysMap = new Map(); // dayStr -> [sessionNames]
      vangDays.forEach(v => {
        if (!daysMap.has(v.dayStr)) daysMap.set(v.dayStr, []);
        daysMap.get(v.dayStr).push(v.sessionName);
      });

      // Tạo note cho từng ngày
      for (const [dayStr, sessionNames] of daysMap.entries()) {
        const sessionNamesStr = Array.from(new Set(sessionNames))
          .map(s => humanForSession(s))
          .join(', ');
        notes.push(`- Vắng ngày ${dayStr}/${month} (${sessionNamesStr})`);
      }
    }

    // Luôn set data, kể cả khi count = 0 (để reset về 0)
    vangOffData.set(r0, { count: vangCount, notes: notes });
  }

  Logger.log("4) Calculated counts for " + vangOffData.size + " employees");

  if (dryRun) {
    Logger.log("PREVIEW (dryRun) - Sample counts:");
    let i = 0;
    for (const [r0, data] of vangOffData.entries()) {
      const emp = Array.from(masterInfo.empToRow.entries()).find(([e, r]) => r === r0 + 1);
      Logger.log(`  Row ${r0 + 1} (${emp ? emp[0] : '?'}): ${data.count} ca vắng`);
      if (data.notes.length > 0) {
        Logger.log(`    Notes: ${data.notes.join('; ')}`);
      }
      if (++i >= 20) break;
    }
    return { employeesCount: vangOffData.size, dryRun: true };
  }

  // Cập nhật vào sheet
  Logger.log("5) Reading existing values from columns Y and AA...");
  const dataRowCount = lastEmpRow - 1; // Số hàng dữ liệu (bỏ qua hàng header)

  // Đọc giá trị hiện có từ cột Y và AA
  const existingVangOffValues = masterSh.getRange(2, headerMap.vangOffCol, dataRowCount, 1).getValues().map(r => r[0]);
  const existingDetail4Values = masterSh.getRange(2, headerMap.detail4Col, dataRowCount, 1).getValues().map(r => r[0]);

  const vangOffValues = existingVangOffValues.slice();
  const detail4Values = existingDetail4Values.slice();

  Logger.log("6) Updating column Y (VẮNG OFF) and column AA (Chi tiết(4))...");
  let updatedCount = 0;
  for (const [r0, data] of vangOffData.entries()) {
    const rowNum = r0 + 1;
    if (rowNum > lastEmpRow || rowNum === 1) continue;

    const dataIndex = rowNum - 2; // Hàng 2 -> index 0
    if (dataIndex >= 0 && dataIndex < vangOffValues.length) {
      // Cập nhật cột Y: ghi đè số lượng
      vangOffValues[dataIndex] = data.count;

      // Cập nhật cột AA: append vào nội dung hiện có
      const existingNote = String(existingDetail4Values[dataIndex] || '').trim();
      const newNote = data.notes.join('\n');
      if (newNote) {
        // Xóa các note vắng cũ (nếu có) để tránh trùng lặp
        const existingLines = existingNote.split('\n').filter(line => {
          const trimmed = line.trim();
          return trimmed && !trimmed.startsWith('- Vắng ngày');
        });
        const combinedNote = existingLines.length > 0
          ? existingLines.join('\n') + '\n' + newNote
          : newNote;
        detail4Values[dataIndex] = combinedNote;
        Logger.log(`  Row ${rowNum} AA note: ${newNote}`);
      } else if (data.count === 0) {
        // Nếu không có vắng, xóa các note vắng cũ (nếu có)
        const existingLines = String(existingDetail4Values[dataIndex] || '').split('\n').filter(line => {
          const trimmed = line.trim();
          return trimmed && !trimmed.startsWith('- Vắng ngày');
        });
        if (existingLines.length > 0) {
          detail4Values[dataIndex] = existingLines.join('\n');
        }
      }

      Logger.log(`  Row ${rowNum}: ${data.count} ca vắng, notes: ${data.notes.length} dòng`);
      updatedCount++;
    }
  }

  // Ghi vào sheet (bắt đầu từ hàng 2)
  try {
    // Ghi cột Y (VẮNG OFF)
    const vangOffRange = masterSh.getRange(2, headerMap.vangOffCol, dataRowCount, 1);
    vangOffRange.setValues(vangOffValues.map(x => [x || 0]));
    Logger.log("7a) Updated " + updatedCount + " employees in column Y (VẮNG OFF)");

    // Ghi cột AA (Chi tiết(4))
    if (headerMap.detail4Col) {
      const detail4Range = masterSh.getRange(2, headerMap.detail4Col, dataRowCount, 1);
      detail4Range.setValues(detail4Values.map(x => [x || '']));
      Logger.log("7b) Updated " + updatedCount + " employees in column AA (Chi tiết(4)) at column " + headerMap.detail4Col);
    } else {
      Logger.log("WARNING: detail4Col not found, skipping column AA update");
    }
  } catch (e) {
    Logger.log(`ERROR writing columns: ${e.message}`);
    throw e;
  }

  try {
    const ui = SpreadsheetApp.getUi();
    ui.alert(`Hoàn thành! Đã cập nhật ${updatedCount} nhân viên:\n- Cột Y (VẮNG OFF): Số lượng ca vắng\n- Cột AA (Chi tiết(4)): Lý do vắng`);
  } catch (e) {
    Logger.log(`Alert skipped (no UI available). Updated ${updatedCount} rows in columns Y and AA.`);
  }

  return { employeesCount: updatedCount, dryRun: false };
}

/**
 * Helper function để ghi trực tiếp vào sheet (không preview)
 * Gọi hàm này để cập nhật cột Y (VẮNG OFF)
 */
function updateVangOffColumnCommit() {
  Logger.log('Running commit: updateVangOffColumn(dryRun=false)');
  return updateVangOffColumn(false);
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
  // Cột F (index 5): EM CHẤM CÔNG CHO NGÀY NÀO - chứa Date object
  // Cột G (index 6): CA LÀM VIỆC CỦA EM - chứa "Check in ca sáng" hoặc "Check out ca chiều"

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const timestamp = row[0]; // Cột A
    const fullName = String(row[2] || '').trim(); // Cột C
    const checkType = String(row[4] || '').trim(); // Cột E
    const checkDate = row[5]; // Cột F - Date object
    const workShift = String(row[6] || '').trim(); // Cột G

    // Chỉ lấy các entry có "CA ONLINE"
    if (!checkType.toUpperCase().includes('ONLINE')) continue;
    if (!fullName) continue;

    // Parse ngày từ cột F
    let dateStr = null, dayStr = null, monthStr = null;
    if (checkDate instanceof Date && !isNaN(checkDate.getTime())) {
      const day = checkDate.getDate();
      const month = checkDate.getMonth() + 1;
      dayStr = String(day);
      monthStr = String(month);
      dateStr = `${day}/${month}`;
    }
    if (!dateStr) continue;

    // Parse ca và action từ cột G
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
    if (!timestamp || !(timestamp instanceof Date)) continue;

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
      const dayStr = match[1];
      dayToCol.set(dayStr, c);
      Logger.log(`  Found column for day ${dayStr}-onl at index ${c} (column ${onlStartCol + c})`);
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

    if (!empDayData.has(empCode)) {
      empDayData.set(empCode, new Map());
    }
    const dayMap = empDayData.get(empCode);

    if (!dayMap.has(entry.day)) {
      dayMap.set(entry.day, {
        morning: { in: null, out: null },
        afternoon: { in: null, out: null }
      });
    }

    const dayData = dayMap.get(entry.day);
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
    for (const [dayStr, sessions] of daySched.entries()) {
      const colIndex = dayToCol.get(dayStr);
      if (colIndex === undefined) {
        Logger.log(`Warning: Không tìm thấy cột cho ngày ${dayStr}-onl`);
        continue;
      }

      const dayData = empDayData.get(empCode)?.get(dayStr);
      if (!dayData) {
        // Không có dữ liệu check in/out cho ngày này
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

/**
 * Hàm debug để kiểm tra một ô cụ thể trong sheet
 * @param {string} empCode - Mã nhân viên (ví dụ: MH0010)
 * @param {string} dayStr - Ngày (ví dụ: "8")
 */
function debugOnlCell(empCode, dayStr) {
  const MASTER_FILE_ID = '1kgPdAK4WxNE7bQSD7Oo62_fnf9WsUoGGyTgQZhJRFU4';
  const MASTER_SHEET_NAME = 'Chấm công th12/2025';
  const onlStartCol = 119; // DO

  const ss = SpreadsheetApp.openById(MASTER_FILE_ID);
  const masterSh = ss.getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) {
    Logger.log(`ERROR: Không tìm thấy sheet ${MASTER_SHEET_NAME}`);
    return;
  }

  // Tìm cột cho ngày
  const header = masterSh.getRange(1, onlStartCol, 1, 31).getValues()[0];
  let colIndex = null;
  for (let c = 0; c < header.length; c++) {
    const headerVal = String(header[c] || '').trim();
    const match = headerVal.match(/^(\d{1,2})-onl$/i);
    if (match && match[1] === dayStr) {
      colIndex = c;
      break;
    }
  }

  if (colIndex === null) {
    Logger.log(`ERROR: Không tìm thấy cột cho ngày ${dayStr}-onl`);
    return;
  }

  // Tìm hàng cho nhân viên
  const masterInfo = buildMasterInfo_(masterSh, 2, 1);
  const row1 = masterInfo.empToRow.get(empCode.toUpperCase());
  if (!row1) {
    Logger.log(`ERROR: Không tìm thấy nhân viên ${empCode}`);
    return;
  }

  const actualCol = onlStartCol + colIndex;
  const colLetter = columnNumberToLetter_(actualCol);
  const cell = masterSh.getRange(row1, actualCol);

  Logger.log(`=== DEBUG CELL ===`);
  Logger.log(`Employee: ${empCode}`);
  Logger.log(`Day: ${dayStr}`);
  Logger.log(`Cell: ${colLetter}${actualCol} (row ${row1}, col ${actualCol})`);
  Logger.log(`Value: "${cell.getValue()}"`);
  Logger.log(`Display Value: "${cell.getDisplayValue()}"`);
  Logger.log(`Formula: "${cell.getFormula()}"`);
  Logger.log(`Number Format: "${cell.getNumberFormat()}"`);
  Logger.log(`URL: https://docs.google.com/spreadsheets/d/${MASTER_FILE_ID}/edit#gid=${masterSh.getSheetId()}&range=${colLetter}${row1}`);
}

/**
 * IMPORT GOOGLE FORM CHẤM CÔNG ONLINE -> SHEET TỔNG (Cột DO -> ES)
 * Xử lý CA ONLINE: fill vào cột DO -> ES với format "onl ca sáng", "onl ca chiều", "onl 2 ca"
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
  
  // Mapping cột trong form responses (0-based index)
  const FORM_COL_TIMESTAMP = 0;      // A: Dấu thời gian
  const FORM_COL_EMAIL = 1;           // B: Email
  const FORM_COL_EMP_CODE = 2;        // C: Mã nhân viên (đã sửa từ D sang C)
  const FORM_COL_NAME = 3;            // D: Họ và tên
  const FORM_COL_TEAM = 4;            // E: Team
  const FORM_COL_DATE = 5;            // F: Ngày chấm công
  const FORM_COL_SHIFT = 6;           // G: Ca làm việc
  const FORM_COL_PROOF = 7;           // H: Minh chứng
  const FORM_COL_TYPE = 9;            // J: EM CHẤM CÔNG CHO HÌNH THỨC (CA ONLINE / CA OFFLINE - CƠ SỞ KHÁC)
  const FORM_COL_WORK_TYPE = 8;       // I: Hình thức làm việc (Parttime/Fulltime/Online)
  
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
    
    // Lấy ngày chấm công từ cột A (Dấu thời gian) và kiểm tra tháng 12/2025
    const dateValue = row[FORM_COL_TIMESTAMP];
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
    // Kiểm tra nếu shiftType là Date object thì bỏ qua (có thể đang lấy từ cột sai)
    const shiftTypeRaw = row[actualShiftCol];
    if (shiftTypeRaw instanceof Date) {
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, day=${dayStr}: Shift column ${actualShiftCol} contains Date object instead of shift type text. Skipping.`);
      continue;
    }
    const shiftType = String(shiftTypeRaw || "").trim();
    const timestamp = row[FORM_COL_TIMESTAMP];
    
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
  Logger.log(`   Stats: processed=${processed}, skippedType=${skippedType}, skippedEmp=${skippedEmp}, skippedMonth=${skippedMonth}, skippedDate=${skippedDate}, skippedTime=${skippedTime}`);
  
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
 * Xử lý CA OFFLINE - CƠ SỞ KHÁC: fill vào cột CB -> DF với format "off ca sáng", "off ca chiều", "off 2 ca"
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
  
  // Mapping cột trong form responses (0-based index)
  const FORM_COL_TIMESTAMP = 0;      // A: Dấu thời gian
  const FORM_COL_EMAIL = 1;           // B: Email
  const FORM_COL_EMP_CODE = 2;        // C: Mã nhân viên (đã sửa từ D sang C)
  const FORM_COL_NAME = 3;            // D: Họ và tên
  const FORM_COL_TEAM = 4;            // E: Team
  const FORM_COL_DATE = 5;            // F: Ngày chấm công
  const FORM_COL_SHIFT = 6;           // G: Ca làm việc
  const FORM_COL_PROOF = 7;           // H: Minh chứng
  const FORM_COL_TYPE = 9;            // J: EM CHẤM CÔNG CHO HÌNH THỨC (CA ONLINE / CA OFFLINE - CƠ SỞ KHÁC)
  const FORM_COL_WORK_TYPE = 8;       // I: Hình thức làm việc (Parttime/Fulltime/Online)
  
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
    
    // Lấy ngày chấm công từ cột A (Dấu thời gian) và kiểm tra tháng 12/2025
    const dateValue = row[FORM_COL_TIMESTAMP];
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
    // Kiểm tra nếu shiftType là Date object thì bỏ qua (có thể đang lấy từ cột sai)
    const shiftTypeRaw = row[actualShiftCol];
    if (shiftTypeRaw instanceof Date) {
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, day=${dayStr}: Shift column ${actualShiftCol} contains Date object instead of shift type text. Skipping.`);
      continue;
    }
    const shiftType = String(shiftTypeRaw || "").trim();
    const timestamp = row[FORM_COL_TIMESTAMP];
    
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
  Logger.log(`   Stats: processed=${processed}, skippedType=${skippedType}, skippedEmp=${skippedEmp}, skippedMonth=${skippedMonth}, skippedDate=${skippedDate}, skippedTime=${skippedTime}`);
  
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