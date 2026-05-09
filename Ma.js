// ================================================================
//  CẤU HÌNH CHUNG – Chỉnh tại đây khi chuyển tháng / đổi link sheet
// ================================================================
const CONFIG = Object.freeze({

  // ── Tháng / năm (hiển thị / tên sheet; không dùng để lọc 1 tháng khi import form) ──
  MONTH: 4,     // Tháng kết thúc chu kỳ (vd tháng lương)
  YEAR: 2026,

  // ── Chu kỳ lương (31 cột mỗi khối): DO–ES "29-onl"…"28-onl"; CB–DF "29-ngoài"…"28-ngoài" ──
  // Cùng một khoảng ngày: cuối tháng 3 (29–31) nối đầu tháng 4 (1–28). Ví dụ: 29/03/2026 → 28/04/2026
  CYCLE_START_DAY: 29,
  CYCLE_START_MONTH: 3,
  CYCLE_START_YEAR: 2026,
  CYCLE_END_DAY: 28,
  CYCLE_END_MONTH: 4,
  CYCLE_END_YEAR: 2026,

  // ── Google Sheet IDs ─────────────────────────────────────────
  // Rawlog máy chấm công (chứa các sheet cơ sở)
  RAW_FILE_ID: "1_XreA3-PYrl_1dJf17bVPObLitFA85j2hkMSk5IZlhw",
  // Sheet chấm công tổng (ghi kết quả)
  MASTER_FILE_ID: "1CPmoryFa-uX6Gsu8dYLKf5_UAIc0Hmfdl3IGrGE-YY8",
  // Google Form chấm công (dùng chung cho cả ONL lẫn OFF ngoài)
  FORM_FILE_ID: "1m_cnd056BLf-w3jhR-e7ZKwgQGN5u9aBzqAHTsDPUdY",

  // ── Tên sheet ────────────────────────────────────────────────
  // Sheet tổng trong MASTER_FILE_ID
  MASTER_SHEET_NAME: "Chấm công th4/2026 29/3-28/4",
  // Sheet form ONL trong FORM_FILE_ID
  ONL_FORM_SHEET_NAME: "CHẤM CÔNG FORM T4/26",
  // Sheet form OFF ngoài trong FORM_FILE_ID
  OFF_FORM_SHEET_NAME: "CHẤM CÔNG FORM T4/26",

  // ── Tên các sheet rawlog (cơ sở) trong RAW_FILE_ID ──────────
  // RAW_SHEETS: ["TRỆT TP", "LẦU 1 TP", "HDK"],
  RAW_SHEETS: ["4. TRỆT TP", "4. LẦU 1 TP", "4. HDK"],

  // ── Cột cố định trong sheet tổng (1-based) ───────────────────
  MASTER_EMP_COL: 2,     // Cột mã nhân viên (B)
  MASTER_HEADER_ROW: 1,  // Hàng header chứa số ngày 1–31

  // Vân tay OFF: cột AJ (36) → BN (66)
  FINGERPRINT_COL_START: 36,
  FINGERPRINT_COL_END: 66,

  // Form OFF ngoài: CB (80) → DF (110), header hàng 1 kiểu 29-ngoài … 28-ngoài (không phải ngày 1–31 liên tục)
  OFFLINE_COL_START: 80,
  OFFLINE_COL_END: 110,

  // Form ONL: cột DO (119) → ES (149)
  ONLINE_COL_START: 119,
  ONLINE_COL_END: 149,

  // Cột ghi tổng ca (1-based)
  FINGERPRINT_TOTAL_COL: 73,   // BU – tổng ca OFF vân tay
  ONL_TOTAL_COL: 156,  // EZ – tổng ca ONL form
  OFF_TOTAL_COL: 116,  // DL – tổng ca OFF form

  // ── Giờ làm mặc định ─────────────────────────────────────────
  MORNING_START: "08:30",
  AFTERNOON_START: "13:15",
  CUTOFF: "12:00",   // Giờ cắt sáng/chiều
  LATE_THRESHOLD: 30,        // Phút trễ tối đa (> 30 = quên chấm)
  MAX_TIMES_PER_DAY: 4,        // Số lần quẹt tối đa/ngày (>4 = bất thường)
});

/** @return {{ y: number, m: number, d: number }|null} */
function parseWorkDateFromFormValue_(dateValue) {
  if (dateValue instanceof Date && !isNaN(dateValue.getTime())) {
    return { y: dateValue.getFullYear(), m: dateValue.getMonth() + 1, d: dateValue.getDate() };
  }
  const dateStr = String(dateValue || "").trim();
  if (!dateStr) return null;
  const m1 = dateStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s|$)/);
  const m2 = !m1 ? dateStr.match(/(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})(?:\s|$)/) : null;
  const m3 = (!m1 && !m2) ? dateStr.match(/(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})(?:\s|$)/) : null;
  if (m1) {
    const a1 = parseInt(m1[1], 10), a2 = parseInt(m1[2], 10), y = parseInt(m1[3], 10);
    // DD/MM/YYYY (VN)
    const t1 = new Date(y, a2 - 1, a1).getTime();
    if (!isNaN(t1) && t1 > 0) return { y, m: a2, d: a1 };
  }
  if (m2) {
    const y = parseInt(m2[1], 10), mo = parseInt(m2[2], 10), d = parseInt(m2[3], 10);
    return { y, m: mo, d };
  }
  if (m3) {
    const d = parseInt(m3[1], 10), mo = parseInt(m3[2], 10), y = parseInt(m3[3], 10);
    return { y, m: mo, d };
  }
  return null;
}

function getPayCycleStartEndMs_() {
  const s = new Date(CONFIG.CYCLE_START_YEAR, CONFIG.CYCLE_START_MONTH - 1, CONFIG.CYCLE_START_DAY).getTime();
  const e = new Date(CONFIG.CYCLE_END_YEAR, CONFIG.CYCLE_END_MONTH - 1, CONFIG.CYCLE_END_DAY).getTime();
  return { cycleStartMs: s, cycleEndMs: e };
}

/** Offset 0..30 trong khối 31 cột ONL (DO–ES) hoặc OFF ngoài (CB–DF): cùng CONFIG.CYCLE_* (29 → 0 … 28 tháng sau → 30). */
function payCycleColumnOffset_(y, m, d) {
  const { cycleStartMs, cycleEndMs } = getPayCycleStartEndMs_();
  const dMs = new Date(y, m - 1, d).getTime();
  if (dMs < cycleStartMs || dMs > cycleEndMs) return -1;
  return Math.round((dMs - cycleStartMs) / 86400000);
}

function dateKeyYmd_(y, m, d) {
  return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

/**
 * Cột ngày làm việc: ưu tiên header "ngày nào…"; nếu form có 2 cột "EM CHẤM CÔNG CHO" thì chọn cột
 * mà vài dòng đầu parse được ngày (F thường là ngày, E là CA ONLINE).
 */
function detectFormDateColumn_(formValues) {
  if (!formValues || !formValues.length) return 6;
  const headerRow = formValues[0];
  for (let c = 0; c < headerRow.length; c++) {
    const headerText = String(headerRow[c] || "").toLowerCase().trim();
    if (headerText.includes("ngày nào") || headerText.includes("ngay nao") ||
        headerText.includes("cho ngày") || headerText.includes("cho ngay") ||
        (headerText.includes("ngày ca") && headerText.includes("làm"))) {
      return c;
    }
  }
  const candidates = [];
  for (let c = 0; c < headerRow.length; c++) {
    const h = String(headerRow[c] || "").toLowerCase();
    if (h.includes("chấm công cho") && !h.includes("hình thức") && !h.includes("hinh thuc") &&
        !h.includes("ca online") && !h.includes("ca offline")) {
      candidates.push(c);
    }
  }
  for (const c of candidates) {
    let ok = 0;
    for (let r = 1; r < Math.min(8, formValues.length); r++) {
      if (parseWorkDateFromFormValue_(formValues[r][c])) ok++;
    }
    if (ok >= 2) return c;
  }
  if (candidates.length === 1) return candidates[0];
  if (candidates.length > 1) return candidates[candidates.length - 1];
  return 6;
}

function importAllBranchesRawLogToMaster() {
  const RAW_FILE_ID = CONFIG.RAW_FILE_ID;
  const RAW_SHEETS = CONFIG.RAW_SHEETS.slice();
  const MASTER_FILE_ID = CONFIG.MASTER_FILE_ID;
  const MASTER_SHEET_NAME = CONFIG.MASTER_SHEET_NAME;
  const MASTER_EMP_COL = CONFIG.MASTER_EMP_COL;
  const MASTER_DAY_FIRST_COL = CONFIG.FINGERPRINT_COL_START;  // AJ
  const MASTER_DAY_LAST_COL = CONFIG.FINGERPRINT_COL_END;    // BN

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

  // ====== BÁO CÁO KẾT QUẢ IMPORT RAW LOG ======
  if (notFound.length > 0) {
    Logger.log(`⚠ CẢNH BÁO: ${notFound.length} mã có data vân tay nhưng KHÔNG TÌM THẤY trong sheet tổng:`);
    notFound.forEach((code, i) => Logger.log(`   ${i + 1}. ${code}`));
  } else {
    Logger.log(`✓ Tất cả ${timesByEmpDay.size} mã vân tay đều tìm thấy trong sheet tổng.`);
  }

  try {
    const message = `✓ Import rawlog: ${updatedCells} ô vào cột AJ-BN` +
      (notFound.length ? ` | ⚠ ${notFound.length} mã không có trong sheet tổng: ${notFound.slice(0, 5).join(", ")}${notFound.length > 5 ? '...' : ''}` : "") +
      (errorCount > 0 ? ` | ${errorCount} batch lỗi` : "");
    SpreadsheetApp.getActiveSpreadsheet().toast(message, "Import Raw Log", 10);
    Logger.log(`Toast: ${message}`);
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

  // Một số sheet raw mới ghi "Mã số:", "Mã số nhân viên", hoặc merge lệch sang cột B/C.
  // Vì vậy nhận diện block start theo cụm "ma so" trong vài cột đầu thay vì so khớp tuyệt đối cột A.
  const isBlockStart = (row) => {
    const maxProbe = Math.min(4, row ? row.length : 0); // quét A..D
    for (let c = 0; c < maxProbe; c++) {
      const v = normalize_(row[c]);
      if (!v) continue;
      if (v === "ma so" || /^ma so\b/.test(v) || v.includes("ma so")) return true;
    }
    return false;
  };

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
    // Ô trống / format thời gian đôi khi trả về 0 → không được coi là 00:00 (tránh highlight ô trống)
    if (cell === 0) return [];
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

  if (minDayCol === null) {
    if (dayColMin != null && dayColMax != null) {
      // Header ngày không có trong range → giả định layout tuần tự:
      // ngày 1 = cột dayColMin, ngày 2 = dayColMin+1, ..., ngày 31 = dayColMax
      Logger.log(`   WARN buildMasterInfo_: không tìm thấy header ngày trong [${dayColMin},${dayColMax}], dùng sequential fallback`);
      const numDays = Math.min(dayColMax - dayColMin + 1, 31);
      for (let i = 0; i < numDays; i++) colByDay.set(String(i + 1), dayColMin + i);
      minDayCol = dayColMin;
      maxDayCol = dayColMin + numDays - 1;
    }
    // Nếu null,null mà không thấy ngày → dayBlock rỗng, không throw
    // (import functions chỉ cần empToRow, không cần dayBlock)
  }

  const dayColsCount = minDayCol !== null ? maxDayCol - minDayCol + 1 : 0;
  const dayBlock = minDayCol !== null
    ? masterSh.getRange(1, minDayCol, lastEmpRow, dayColsCount).getValues()
    : [];

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

  // Cột TEAM (F): header "TEAM", giá trị "Page" = Team Page (áp dụng ca tối)
  let teamCol = null;
  for (let c = 0; c < header.length; c++) {
    if (normalize_(header[c]).includes('team')) { teamCol = c + 1; break; }
  }
  const empToTeam = new Map();
  const rowToEmp = new Map();
  if (teamCol !== null) {
    const teamVals = masterSh.getRange(1, teamCol, lastEmpRow, 1).getValues().flat();
    for (const [emp, row] of empToRow.entries()) {
      const v = String(teamVals[row - 1] || '').trim();
      empToTeam.set(emp, v);
      rowToEmp.set(row, emp);
    }
  }
  for (const [emp, row] of empToRow.entries()) {
    if (!rowToEmp.has(row)) rowToEmp.set(row, emp);
  }

  return { lastEmpRow, empToRow, rowToEmp, colByDay, minDayCol, maxDayCol, dayColsCount, dayBlock, header, empToRole, empToTeam };
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
  const MASTER_FILE_ID = CONFIG.MASTER_FILE_ID;
  const MASTER_SHEET_NAME = CONFIG.MASTER_SHEET_NAME;
  const cfg = {
    morningStart: CONFIG.MORNING_START,
    afternoonStart: CONFIG.AFTERNOON_START,
    cutoff: CONFIG.CUTOFF,
    lateThreshold: CONFIG.LATE_THRESHOLD,
    maxTimesThreshold: CONFIG.MAX_TIMES_PER_DAY,
  };

  Logger.log("1) Loading data...");
  // Chỉ highlight theo NỘI DUNG Ô TRÊN SHEET TỔNG (không fallback rawlog): fallback raw khiến ô master trống
  // vẫn bị tô đỏ nếu file raw còn giờ — trông như lỗi “ô không có data”.
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  const masterInfo = buildMasterInfo_(masterSh, CONFIG.MASTER_EMP_COL, CONFIG.MASTER_HEADER_ROW,
    CONFIG.FINGERPRINT_COL_START, CONFIG.FINGERPRINT_COL_END);
  const month = parseMonthFromSheetName_(MASTER_SHEET_NAME) || CONFIG.MONTH;

  // Xóa màu cũ toàn bộ vùng vân tay (AJ–BN) trước khi highlight lại
  Logger.log("1b) Clearing old highlights in fingerprint columns...");
  const clearStartCol = masterInfo.minDayCol || CONFIG.FINGERPRINT_COL_START;
  const clearColCount = (masterInfo.maxDayCol || CONFIG.FINGERPRINT_COL_END) - clearStartCol + 1;
  // Phải gồm đến hàng NHÂN VIÊN CUỐI (lastEmpRow). Trước đây dùng lastEmpRow-1 → sót 1 hàng, màu đỏ cũ không xóa.
  masterSh.getRange(2, clearStartCol, masterInfo.lastEmpRow, clearColCount).setBackground(null);
  SpreadsheetApp.flush();

  const problematicCells = [];
  const threshold = cfg.maxTimesThreshold || 4;

  Logger.log("2) Checking ALL employees in master sheet...");

  // Managers (MH0001-MH0009) có giờ vào ca sáng khác: 09:00 thay vì 08:30
  const MANAGER_IDS = new Set(["MH0001", "MH0002", "MH0003", "MH0004", "MH0005", "MH0006", "MH0007", "MH0008", "MH0009"]);
  const MORNING_START_MIN = timeStrToMinutes_(cfg.morningStart);    // 08:30 = 510
  const MANAGER_START_MIN = timeStrToMinutes_("09:00");             // 540
  const AFTERNOON_START_MIN = timeStrToMinutes_(cfg.afternoonStart);  // 13:15 = 795
  const CUTOFF_MIN = timeStrToMinutes_(cfg.cutoff);          // 12:00 = 720
  const LATE_THRESHOLD = cfg.lateThreshold || 30;
  // Giờ check-in chiều tối đa để phân biệt với check-out sáng: 12:00 – 14:30
  const AFTERNOON_CHECKIN_MAX_MIN = timeStrToMinutes_("14:30");       // 870

  // Duyệt TẤT CẢ nhân viên trong master sheet
  for (const [emp, r1] of masterInfo.empToRow.entries()) {
    const r0 = r1 - 1;
    const isManager = MANAGER_IDS.has(emp.toUpperCase());
    const mornStart = isManager ? MANAGER_START_MIN : MORNING_START_MIN;

    // Duyệt tất cả các ngày trong tháng (1-31)
    for (let day = 1; day <= 31; day++) {
      const dayStr = String(day);
      const col1 = masterInfo.colByDay.get(dayStr);
      if (!col1) continue;

      const c0 = col1 - masterInfo.minDayCol;
      if (c0 < 0 || c0 >= masterInfo.dayColsCount) continue;

      // Chỉ dùng dữ liệu trong ô master (đã import raw vào đây). Không đọc raw file — tránh ô trống vẫn highlight.
      let timesArr = [];
      const existing = masterInfo.dayBlock[r0][c0];
      const extracted = extractTimesFromCell_(existing);
      if (!extracted || !extracted.length) {
        const fallback = extractTimes_(String(existing || ""));
        timesArr = fallback && fallback.length ? fallback : [];
      } else {
        timesArr = extracted;
      }

      if (!timesArr || timesArr.length === 0) continue;

      const isTeamPage = !!(masterInfo.empToTeam && (masterInfo.empToTeam.get(emp) || '').trim().toLowerCase() === 'page');
      const maxTimesAllowed = isTeamPage ? 6 : threshold;

      // Kiểm tra 1: quá số lần quẹt cho phép
      if (timesArr.length > maxTimesAllowed) {
        problematicCells.push({ r0, c0, emp, dayStr, type: 'tooManyTimes', timesCount: timesArr.length });
        Logger.log(`FOUND >${maxTimesAllowed} times: emp=${emp} day=${dayStr} times=${timesArr.length}`);
        continue;
      }

      // Kiểm tra 2: Check-in trễ (không dùng schedule phức tạp)
      // – Ca sáng: lấy giờ sớm nhất trước 12:00
      const morningTimes = timesArr.filter(t => timeStrToMinutes_(t) < CUTOFF_MIN);
      if (morningTimes.length > 0) {
        const checkIn = morningTimes[0];
        const lateMin = timeStrToMinutes_(checkIn) - mornStart;
        if (lateMin > 0) {
          problematicCells.push({ r0, c0, emp, dayStr, type: 'late', sessionName: 'morning', lateMinutes: lateMin, checkInTime: checkIn });
          Logger.log(`FOUND late morning: emp=${emp} day=${dayStr} late=${lateMin}min checkIn=${checkIn}`);
        }
      }

      // – Ca chiều: lấy giờ sớm nhất trong khoảng 12:00–14:30
      const afternoonTimes = timesArr.filter(t => {
        const m = timeStrToMinutes_(t);
        return m >= CUTOFF_MIN && m <= AFTERNOON_CHECKIN_MAX_MIN;
      });
      if (afternoonTimes.length > 0) {
        const checkIn = afternoonTimes[0];
        const lateMin = timeStrToMinutes_(checkIn) - AFTERNOON_START_MIN;
        if (lateMin > 0) {
          problematicCells.push({ r0, c0, emp, dayStr, type: 'late', sessionName: 'afternoon', lateMinutes: lateMin, checkInTime: checkIn });
          Logger.log(`FOUND late afternoon: emp=${emp} day=${dayStr} late=${lateMin}min checkIn=${checkIn}`);
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

/**
 * Cập nhật cột BU (TỔNG ca off vân tay) = số ca làm trong tháng, đếm từ dữ liệu vân tay (cột ngày AJ-BN).
 * Mỗi ca có đủ check-in và check-out được tính 1 ca.
 * @param {boolean} dryRun - Nếu true chỉ preview, không ghi vào sheet
 * @return {Object} - { employeesCount, updatedCount, dryRun?, buCol? }
 */
function updateTongCaOffVanTay(dryRun = true) {
  const MASTER_FILE_ID = CONFIG.MASTER_FILE_ID;
  const MASTER_SHEET_NAME = CONFIG.MASTER_SHEET_NAME;
  const CUTOFF_MIN = timeStrToMinutes_(CONFIG.CUTOFF); // phút cắt sáng/chiều (720 = 12:00)

  Logger.log("[updateTongCaOffVanTay] 1) Opening master sheet...");
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) throw new Error("Không tìm thấy sheet tổng: " + MASTER_SHEET_NAME);

  const masterInfo = buildMasterInfo_(masterSh, CONFIG.MASTER_EMP_COL, CONFIG.MASTER_HEADER_ROW,
    CONFIG.FINGERPRINT_COL_START, CONFIG.FINGERPRINT_COL_END);
  const headerMap = findHeaderCols_(masterInfo.header);
  const buCol = headerMap.totalCaOffVanTayCol;
  if (!buCol) {
    throw new Error("Không tìm thấy cột TỔNG ca off vân tay (BU) trong sheet tổng");
  }

  const lastEmpRow = masterInfo.lastEmpRow;
  const totals = []; // totals[r0] = số ca làm trong tháng

  Logger.log("2) Calculating total sessions per employee (from fingerprint data)...");
  // Đếm ca đơn giản: mỗi ngày có giờ trước 12:00 = 1 ca sáng, có giờ từ 12:00 trở đi = 1 ca chiều
  for (const [emp, r1] of masterInfo.empToRow.entries()) {
    const r0 = r1 - 1;
    let count = 0;
    for (let day = 1; day <= 31; day++) {
      const col1 = masterInfo.colByDay.get(String(day));
      if (!col1) continue;
      const c0 = col1 - masterInfo.minDayCol;
      if (c0 < 0 || c0 >= masterInfo.dayColsCount) continue;
      const cellVal = masterInfo.dayBlock[r0][c0];
      const times = extractTimesFromCell_(cellVal);
      if (!times || times.length === 0) continue;
      const hasMorning = times.some(t => timeStrToMinutes_(t) < CUTOFF_MIN);
      const hasAfternoon = times.some(t => timeStrToMinutes_(t) >= CUTOFF_MIN + 30); // 12:30
      if (hasMorning) count++;
      if (hasAfternoon) count++;
    }
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

// ─── Helper: đếm số ca từ nội dung ô form (ONL/OFF) ────────────────────────
// "onl 2 ca" / "off 2 ca"  → 2 ca
// bất kỳ nội dung khác → 1 ca
// ô trống → 0 ca
function countCaFromFormCell_(cellVal) {
  const s = String(cellVal || "").trim().toLowerCase();
  if (!s) return 0;
  if (s.includes("2 ca")) return 2;
  return 1;
}

/**
 * Đếm tổng ca ONL form từ cột DO-ES và ghi vào cột EX.
 * @param {boolean} dryRun - true = chỉ preview, false = ghi thật
 */
function updateTongCaOnlForm(dryRun = true) {
  const MASTER_FILE_ID = CONFIG.MASTER_FILE_ID;
  const MASTER_SHEET_NAME = CONFIG.MASTER_SHEET_NAME;
  const COL_START = CONFIG.ONLINE_COL_START;   // DO = 119
  const COL_END = CONFIG.ONLINE_COL_END;     // ES = 149
  const TOTAL_COL = CONFIG.ONL_TOTAL_COL;      // EX = 154

  Logger.log("[updateTongCaOnlForm] 1) Opening master sheet...");
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) throw new Error("Không tìm thấy sheet tổng: " + MASTER_SHEET_NAME);

  const masterInfo = buildMasterInfo_(masterSh, CONFIG.MASTER_EMP_COL, CONFIG.MASTER_HEADER_ROW,
    COL_START, COL_END);
  const lastEmpRow = masterInfo.lastEmpRow;
  const dayColsCount = COL_END - COL_START + 1;

  Logger.log("[updateTongCaOnlForm] 2) Calculating total ONL sessions per employee (columns DO-ES)...");
  const totals = [];
  for (const [emp, r1] of masterInfo.empToRow.entries()) {
    const r0 = r1 - 1;
    let count = 0;
    for (let c0 = 0; c0 < dayColsCount; c0++) {
      count += countCaFromFormCell_(masterInfo.dayBlock[r0][c0]);
    }
    totals[r0] = count;
  }

  if (dryRun) {
    Logger.log("PREVIEW (dryRun) - Sample TỔNG ca ONL form (EX):");
    let i = 0;
    for (const [emp, r1] of masterInfo.empToRow.entries()) {
      Logger.log(`  Row ${r1} (${emp}): ${totals[r1 - 1] || 0} ca`);
      if (++i >= 15) break;
    }
    return { employeesCount: masterInfo.empToRow.size, dryRun: true, totalCol: TOTAL_COL };
  }

  Logger.log("[updateTongCaOnlForm] 3) Writing column EX (TỔNG ca ONL form)...");
  const headerValue = masterSh.getRange(1, TOTAL_COL, 1, 1).getValue();
  const writeValues = [[headerValue]];
  for (let r0 = 1; r0 < lastEmpRow; r0++) {
    writeValues.push([totals[r0] != null ? totals[r0] : 0]);
  }
  masterSh.getRange(1, TOTAL_COL, lastEmpRow, 1).setValues(writeValues);

  Logger.log(`[updateTongCaOnlForm] 4) Done. Updated TỔNG ca ONL form (EX) for ${lastEmpRow - 1} rows.`);
  SpreadsheetApp.getActiveSpreadsheet().toast(`✓ Tổng ca ONL form (EX): ${lastEmpRow - 1} nhân viên`, "Hoàn thành", 5);
  return { employeesCount: lastEmpRow - 1, updatedCount: lastEmpRow - 1, dryRun: false, totalCol: TOTAL_COL };
}

function updateTongCaOnlFormCommit() {
  return updateTongCaOnlForm(false);
}

/**
 * Đếm tổng ca OFF ngoài form từ cột CB-DF (29-ngoài…28-ngoài) và ghi vào cột DL.
 * @param {boolean} dryRun - true = chỉ preview, false = ghi thật
 */
function updateTongCaOffForm(dryRun = true) {
  const MASTER_FILE_ID = CONFIG.MASTER_FILE_ID;
  const MASTER_SHEET_NAME = CONFIG.MASTER_SHEET_NAME;
  const COL_START = CONFIG.OFFLINE_COL_START;  // CB = 80
  const COL_END = CONFIG.OFFLINE_COL_END;    // DF = 110
  const TOTAL_COL = CONFIG.OFF_TOTAL_COL;      // DL = 116

  Logger.log("[updateTongCaOffForm] 1) Opening master sheet...");
  const masterSh = SpreadsheetApp.openById(MASTER_FILE_ID).getSheetByName(MASTER_SHEET_NAME);
  if (!masterSh) throw new Error("Không tìm thấy sheet tổng: " + MASTER_SHEET_NAME);

  const masterInfo = buildMasterInfo_(masterSh, CONFIG.MASTER_EMP_COL, CONFIG.MASTER_HEADER_ROW,
    COL_START, COL_END);
  const lastEmpRow = masterInfo.lastEmpRow;
  const dayColsCount = COL_END - COL_START + 1;

  Logger.log("[updateTongCaOffForm] 2) Calculating total OFF form sessions per employee (columns CB-DF)...");
  const totals = [];
  for (const [emp, r1] of masterInfo.empToRow.entries()) {
    const r0 = r1 - 1;
    let count = 0;
    for (let c0 = 0; c0 < dayColsCount; c0++) {
      count += countCaFromFormCell_(masterInfo.dayBlock[r0][c0]);
    }
    totals[r0] = count;
  }

  if (dryRun) {
    Logger.log("PREVIEW (dryRun) - Sample TỔNG ca OFF form (DL):");
    let i = 0;
    for (const [emp, r1] of masterInfo.empToRow.entries()) {
      Logger.log(`  Row ${r1} (${emp}): ${totals[r1 - 1] || 0} ca`);
      if (++i >= 15) break;
    }
    return { employeesCount: masterInfo.empToRow.size, dryRun: true, totalCol: TOTAL_COL };
  }

  Logger.log("[updateTongCaOffForm] 3) Writing column DL (TỔNG ca OFF form)...");
  const headerValue = masterSh.getRange(1, TOTAL_COL, 1, 1).getValue();
  const writeValues = [[headerValue]];
  for (let r0 = 1; r0 < lastEmpRow; r0++) {
    writeValues.push([totals[r0] != null ? totals[r0] : 0]);
  }
  masterSh.getRange(1, TOTAL_COL, lastEmpRow, 1).setValues(writeValues);

  Logger.log(`[updateTongCaOffForm] 4) Done. Updated TỔNG ca OFF form (DL) for ${lastEmpRow - 1} rows.`);
  SpreadsheetApp.getActiveSpreadsheet().toast(`✓ Tổng ca OFF form (DL): ${lastEmpRow - 1} nhân viên`, "Hoàn thành", 5);
  return { employeesCount: lastEmpRow - 1, updatedCount: lastEmpRow - 1, dryRun: false, totalCol: TOTAL_COL };
}

function updateTongCaOffFormCommit() {
  return updateTongCaOffForm(false);
}

/**
 * IMPORT GOOGLE FORM CHẤM CÔNG ONLINE -> SHEET TỔNG (Cột DO -> ES)
 * Map cột theo chu kỳ CONFIG.CYCLE_* (header "29-onl" … "28-onl"), không map theo ngày lịch 1–31.
 * Quy tắc 24h: cột ngày làm detectFormDateColumn_; timestamp (A) cùng ngày với ngày làm.
 */
function importOnlineFormToMaster() {
  const FORM_FILE_ID = CONFIG.FORM_FILE_ID;
  const FORM_SHEET_NAME = CONFIG.ONL_FORM_SHEET_NAME;
  const MASTER_FILE_ID = CONFIG.MASTER_FILE_ID;
  const MASTER_SHEET_NAME = CONFIG.MASTER_SHEET_NAME;
  const MASTER_EMP_COL = CONFIG.MASTER_EMP_COL;
  const MASTER_HEADER_ROW = CONFIG.MASTER_HEADER_ROW;
  const ONLINE_START_COL = CONFIG.ONLINE_COL_START;  // DO
  const ONLINE_END_COL = CONFIG.ONLINE_COL_END;    // ES

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

  Logger.log("[importOnlineFormToMaster] 1) Opening form responses sheet...");
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

  const actualDateCol = detectFormDateColumn_(formValues);
  Logger.log(`   DATE column index (detectFormDateColumn_): ${actualDateCol}`);

  // ====== 2) PARSE FORM DATA – CA ONLINE, ngày trong chu kỳ ${CYCLE_START}→${CYCLE_END} ======
  const { cycleStartMs, cycleEndMs } = getPayCycleStartEndMs_();
  Logger.log(`2) Parsing form data (CA ONLINE only, cycle ${CONFIG.CYCLE_START_DAY}/${CONFIG.CYCLE_START_MONTH}/${CONFIG.CYCLE_START_YEAR} → ${CONFIG.CYCLE_END_DAY}/${CONFIG.CYCLE_END_MONTH}/${CONFIG.CYCLE_END_YEAR})...`);
  // Map: empCode -> Map<dateKey yyyy-MM-dd, {morning, afternoon, evening}>
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

    // Lấy mã nhân viên: thử actualEmpCol trước, nếu không hợp lệ thì extract từ email (col B index 1)
    // Trường hợp form mới: email chứa "minhhasgu MH0088" → extract MH0088
    let empCodeRaw = String(row[actualEmpCol] || "").trim();
    if (!empRegex.test(empCodeRaw)) {
      const emailRaw = String(row[1] || "").trim();
      const mhMatch = emailRaw.match(/\b(MH\d{4,})\b/i);
      if (mhMatch) empCodeRaw = mhMatch[1];
    }
    if (!empCodeRaw) {
      skippedEmp++;
      skippedEmpCodes.add("(empty)");
      continue;
    }
    if (!empRegex.test(empCodeRaw)) {
      skippedEmp++;
      skippedEmpCodes.add(empCodeRaw);
      continue;
    }
    const empCode = empCodeRaw.toUpperCase();
    validEmpCodes.add(empCode);

    // Lấy ngày ca làm việc (chu kỳ CONFIG.CYCLE_*). Quy tắc 24h: timestamp (A) cùng ngày với ngày làm.
    const dateValue = row[actualDateCol];
    const parsedDate = parseWorkDateFromFormValue_(dateValue);
    if (!parsedDate) {
      skippedDate++;
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, date="${dateValue}" (cannot parse)`);
      }
      continue;
    }
    const parsedYear = parsedDate.y;
    const parsedMonth = parsedDate.m;
    const parsedDay = parsedDate.d;
    const workMs = new Date(parsedYear, parsedMonth - 1, parsedDay).getTime();
    if (workMs < cycleStartMs || workMs > cycleEndMs) {
      skippedMonth++;
      if (skippedMonth <= 3) {
        Logger.log(`   DEBUG skippedCycle #${skippedMonth}: emp=${empCode}, raw="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay}`);
      }
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, date="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay} (outside pay cycle)`);
      }
      continue;
    }
    const dateKey = dateKeyYmd_(parsedYear, parsedMonth, parsedDay);

    // Debug: Log vài dòng đầu để xem parse ngày và shift type
    if (processed < 5 && empCode) {
      const debugShift = row[actualShiftCol];
      const debugShiftStr = debugShift instanceof Date ? debugShift.toString() : String(debugShift || "").trim();
      Logger.log(`   DEBUG Row ${r + 1}, emp=${empCode}: dateValue="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay}, dateKey=${dateKey}, shift[${actualShiftCol}]="${debugShiftStr}"`);
    }

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
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, date=${dateKey}: Shift value is Date object. Skipping.`);
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
          Logger.log(`   Skip cross-day entry Row ${r + 1}, emp=${empCode}, date=${dateKey}: workKey=${workKey}, tsKey=${tsKey}, shift="${shiftType}"`);
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
        validButSkipped.push(`emp=${empCode}, date=${dateKey}, timestamp="${timestamp}" (cannot parse)`);
      }
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, date=${dateKey}: Cannot parse timestamp: ${timestamp}`);
      continue;
    }

    processed++;

    // Khởi tạo map nếu chưa có
    if (!onlineByEmpDay.has(empCode)) {
      onlineByEmpDay.set(empCode, new Map());
    }
    const dayMap = onlineByEmpDay.get(empCode);
    if (!dayMap.has(dateKey)) {
      dayMap.set(dateKey, { morning: { in: null, out: null }, afternoon: { in: null, out: null }, evening: { in: null, out: null } });
    }
    const dayData = dayMap.get(dateKey);
    if (!dayData.evening) dayData.evening = { in: null, out: null };

    const shiftLower = shiftType.toLowerCase();
    if (shiftLower.includes("check in ca sáng") || shiftLower.includes("check in ca sang")) {
      dayData.morning.in = timeStr;
    } else if (shiftLower.includes("check out ca sáng") || shiftLower.includes("check out ca sang")) {
      dayData.morning.out = timeStr;
    } else if (shiftLower.includes("check in ca chiều") || shiftLower.includes("check in ca chieu")) {
      dayData.afternoon.in = timeStr;
    } else if (shiftLower.includes("check out ca chiều") || shiftLower.includes("check out ca chieu")) {
      dayData.afternoon.out = timeStr;
    } else if (shiftLower.includes("check in ca tối") || shiftLower.includes("check in ca toi")) {
      dayData.evening.in = timeStr;
    } else if (shiftLower.includes("check out ca tối") || shiftLower.includes("check out ca toi")) {
      dayData.evening.out = timeStr;
    } else {
      const shiftTypeDebug = row[actualShiftCol];
      const shiftTypeDebugStr = shiftTypeDebug instanceof Date ? shiftTypeDebug.toString() : String(shiftTypeDebug || "").trim();
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, date=${dateKey}: Unknown shift type: "${shiftType}" (raw value: "${shiftTypeDebugStr}", column ${actualShiftCol})`);
    }
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

  // Header DO–ES: offset theo chu kỳ (29-onl … 28-onl), không phải ngày 1–31 lịch
  const masterInfo = buildMasterInfo_(masterSh, MASTER_EMP_COL, MASTER_HEADER_ROW, null, null);
  const { empToRow, colByDay } = masterInfo;

  // ====== 4) BUILD DATA TO WRITE ======
  Logger.log("4) Building data to write (columns DO-ES, " + ONLINE_START_COL + "-" + ONLINE_END_COL + ")...");
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

    for (const [dateKey, dayData] of dayMap.entries()) {
      const parts = dateKey.split("-");
      if (parts.length !== 3) {
        Logger.log(`   WARNING: Invalid dateKey "${dateKey}"`);
        continue;
      }
      const py = parseInt(parts[0], 10), pm = parseInt(parts[1], 10), pd = parseInt(parts[2], 10);
      const onlineC0 = payCycleColumnOffset_(py, pm, pd);

      if (onlineC0 < 0 || onlineC0 >= dayColsCount) {
        Logger.log(`   WARNING: date ${dateKey} offset ${onlineC0} is outside range [0, ${dayColsCount - 1}]`);
        continue;
      }

      // Validate: r0 phải trong phạm vi onlineBlock
      if (r0 < 0 || r0 >= dataRowCount) {
        Logger.log(`   WARNING: Row index ${r0} (row ${row1}) is outside range [0, ${dataRowCount - 1}]`);
        continue;
      }

      const targetCol = ONLINE_START_COL + onlineC0;

      const hasMorning = dayData.morning && (dayData.morning.in || dayData.morning.out);
      const hasAfternoon = dayData.afternoon && (dayData.afternoon.in || dayData.afternoon.out);
      const hasEvening = dayData.evening && (dayData.evening.in || dayData.evening.out);
      const segments = [];

      if (hasMorning && hasAfternoon && !hasEvening) {
        const morningTimes = [];
        if (dayData.morning.in) morningTimes.push(dayData.morning.in);
        if (dayData.morning.out) morningTimes.push(dayData.morning.out);
        const afternoonTimes = [];
        if (dayData.afternoon.in) afternoonTimes.push(dayData.afternoon.in);
        if (dayData.afternoon.out) afternoonTimes.push(dayData.afternoon.out);
        segments.push([...morningTimes, ...afternoonTimes].join("\n") + "\nonl 2 ca");
      } else {
        if (hasMorning) {
          const morningTimes = [];
          if (dayData.morning.in) morningTimes.push(dayData.morning.in);
          if (dayData.morning.out) morningTimes.push(dayData.morning.out);
          segments.push(morningTimes.join("\n") + "\nonl ca sáng");
        }
        if (hasAfternoon) {
          const afternoonTimes = [];
          if (dayData.afternoon.in) afternoonTimes.push(dayData.afternoon.in);
          if (dayData.afternoon.out) afternoonTimes.push(dayData.afternoon.out);
          segments.push(afternoonTimes.join("\n") + "\nonl ca chiều");
        }
      }
      if (hasEvening) {
        const eveningTimes = [];
        if (dayData.evening.in) eveningTimes.push(dayData.evening.in);
        if (dayData.evening.out) eveningTimes.push(dayData.evening.out);
        segments.push(eveningTimes.join("\n") + "\nonl ca tối");
      }
      if (segments.length) onlineBlock[r0][onlineC0] = segments.join("\n");

      if (onlineBlock[r0][onlineC0]) {
        updatedCells++;
        // Debug: Log vài cell đầu để kiểm tra mapping
        if (updatedCells <= 5) {
          Logger.log(`   DEBUG: Writing date ${dateKey} (col ${targetCol}, offset ${onlineC0}) for emp ${empCode} at row ${row1}`);
        }
      }
    }
  }

  Logger.log(`   Prepared ${updatedCells} cells to update`);

  // ====== BÁO CÁO MÃ KHÔNG IMPORT ĐƯỢC ======
  if (notFound.length > 0) {
    Logger.log(`⚠ CẢNH BÁO: ${notFound.length} mã có data ONLINE nhưng KHÔNG TÌM THẤY trong sheet tổng:`);
    // Log từng mã để dễ trace
    notFound.forEach((code, i) => Logger.log(`   ${i + 1}. ${code}`));
  } else {
    Logger.log(`✓ Tất cả mã tìm thấy trong sheet tổng.`);
  }
  if (skippedEmpCodes.size > 0) {
    Logger.log(`⚠ ${skippedEmpCodes.size} mã bị bỏ qua vì không hợp lệ (ví dụ: #N/A, trống): ${Array.from(skippedEmpCodes).join(", ")}`);
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
    let message = `✓ Import ONLINE: ${updatedCells} ô vào cột DO-ES`;
    if (notFound.length > 0) message += ` | ⚠ ${notFound.length} mã không có trong sheet tổng: ${notFound.slice(0, 5).join(", ")}${notFound.length > 5 ? '...' : ''}`;
    if (skippedEmpCodes.size > 0) message += ` | ${skippedEmpCodes.size} mã không hợp lệ`;
    if (errorCount > 0) message += ` | ${errorCount} batch lỗi`;
    SpreadsheetApp.getActiveSpreadsheet().toast(message, "Import Online Form", 10);
    Logger.log(`Toast: ${message}`);
  } catch (e) {
    Logger.log(`Finished: Updated ${updatedCells} ô. notFound=${notFound.length}, skippedEmp=${skippedEmpCodes.size}`);
  }
}

/**
 * IMPORT GOOGLE FORM CHẤM CÔNG OFFLINE -> SHEET TỔNG (Cột CB -> DF)
 * Map cột theo chu kỳ CONFIG.CYCLE_* — khớp header "29-ngoài" … "31-ngoài" … "26-ngoài" … "28-ngoài" (31 ô). Quy tắc 24h giống ONL.
 */
function importOfflineFormToMaster() {
  const FORM_FILE_ID = CONFIG.FORM_FILE_ID;
  const FORM_SHEET_NAME = CONFIG.OFF_FORM_SHEET_NAME;
  const MASTER_FILE_ID = CONFIG.MASTER_FILE_ID;
  const MASTER_SHEET_NAME = CONFIG.MASTER_SHEET_NAME;
  const MASTER_EMP_COL = CONFIG.MASTER_EMP_COL;
  const MASTER_HEADER_ROW = CONFIG.MASTER_HEADER_ROW;
  const OFFLINE_START_COL = CONFIG.OFFLINE_COL_START;  // CB
  const OFFLINE_END_COL = CONFIG.OFFLINE_COL_END;    // DF

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

  Logger.log("[importOfflineFormToMaster] 1) Opening form responses sheet...");
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

  const actualDateCol = detectFormDateColumn_(formValues);
  Logger.log(`   DATE column index (detectFormDateColumn_): ${actualDateCol}`);
  const { cycleStartMs, cycleEndMs } = getPayCycleStartEndMs_();

  // ====== 2) PARSE FORM DATA – CA OFFLINE, ngày trong chu kỳ CONFIG.CYCLE_* ======
  Logger.log(`2) Parsing form data (CA OFFLINE only, cycle ${CONFIG.CYCLE_START_DAY}/${CONFIG.CYCLE_START_MONTH}/${CONFIG.CYCLE_START_YEAR} → ${CONFIG.CYCLE_END_DAY}/${CONFIG.CYCLE_END_MONTH}/${CONFIG.CYCLE_END_YEAR})...`);
  // Map: empCode -> Map<dateKey yyyy-MM-dd, …>
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

    // Lấy mã nhân viên: thử actualEmpCol trước, nếu không hợp lệ thì extract từ email (col B index 1)
    let empCodeRaw = String(row[actualEmpCol] || "").trim();
    if (!empRegex.test(empCodeRaw)) {
      const emailRaw = String(row[1] || "").trim();
      const mhMatch = emailRaw.match(/\b(MH\d{4,})\b/i);
      if (mhMatch) empCodeRaw = mhMatch[1];
    }
    if (!empCodeRaw) {
      skippedEmp++;
      skippedEmpCodes.add("(empty)");
      continue;
    }
    if (!empRegex.test(empCodeRaw)) {
      skippedEmp++;
      skippedEmpCodes.add(empCodeRaw);
      continue;
    }
    const empCode = empCodeRaw.toUpperCase();
    validEmpCodes.add(empCode);

    // Lấy ngày ca làm việc (chu kỳ CONFIG.CYCLE_*). Quy tắc 24h: timestamp (A) cùng ngày với ngày làm.
    const dateValue = row[actualDateCol];
    const parsedDate = parseWorkDateFromFormValue_(dateValue);
    if (!parsedDate) {
      skippedDate++;
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, date="${dateValue}" (cannot parse)`);
      }
      continue;
    }
    const parsedYear = parsedDate.y;
    const parsedMonth = parsedDate.m;
    const parsedDay = parsedDate.d;
    const workMs = new Date(parsedYear, parsedMonth - 1, parsedDay).getTime();
    if (workMs < cycleStartMs || workMs > cycleEndMs) {
      skippedMonth++;
      if (skippedMonth <= 3) {
        Logger.log(`   DEBUG skippedCycle #${skippedMonth}: emp=${empCode}, raw="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay}`);
      }
      if (validEmpCodes.has(empCode) && validButSkipped.length < 10) {
        validButSkipped.push(`emp=${empCode}, date="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay} (outside pay cycle)`);
      }
      continue;
    }
    const dateKey = dateKeyYmd_(parsedYear, parsedMonth, parsedDay);

    if (processed < 5 && empCode) {
      const debugShift = row[actualShiftCol];
      const debugShiftStr = debugShift instanceof Date ? debugShift.toString() : String(debugShift || "").trim();
      Logger.log(`   DEBUG Row ${r + 1}, emp=${empCode}: dateValue="${dateValue}", parsed=${parsedYear}/${parsedMonth}/${parsedDay}, dateKey=${dateKey}, shift[${actualShiftCol}]="${debugShiftStr}"`);
    }

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
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, date=${dateKey}: Shift value is Date object. Skipping.`);
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
          Logger.log(`   Skip cross-day entry Row ${r + 1}, emp=${empCode}, date=${dateKey}: workKey=${workKey}, tsKey=${tsKey}, shift="${shiftType}"`);
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
        validButSkipped.push(`emp=${empCode}, date=${dateKey}, timestamp="${timestamp}" (cannot parse)`);
      }
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, date=${dateKey}: Cannot parse timestamp: ${timestamp}`);
      continue;
    }

    processed++;

    // Khởi tạo map nếu chưa có
    if (!offlineByEmpDay.has(empCode)) {
      offlineByEmpDay.set(empCode, new Map());
    }
    const dayMap = offlineByEmpDay.get(empCode);
    if (!dayMap.has(dateKey)) {
      dayMap.set(dateKey, { morning: { in: null, out: null }, afternoon: { in: null, out: null }, evening: { in: null, out: null } });
    }
    const dayData = dayMap.get(dateKey);
    if (!dayData.evening) dayData.evening = { in: null, out: null };

    const shiftLower = shiftType.toLowerCase();
    if (shiftLower.includes("check in ca sáng") || shiftLower.includes("check in ca sang")) {
      dayData.morning.in = timeStr;
    } else if (shiftLower.includes("check out ca sáng") || shiftLower.includes("check out ca sang")) {
      dayData.morning.out = timeStr;
    } else if (shiftLower.includes("check in ca chiều") || shiftLower.includes("check in ca chieu")) {
      dayData.afternoon.in = timeStr;
    } else if (shiftLower.includes("check out ca chiều") || shiftLower.includes("check out ca chieu")) {
      dayData.afternoon.out = timeStr;
    } else if (shiftLower.includes("check in ca tối") || shiftLower.includes("check in ca toi")) {
      dayData.evening.in = timeStr;
    } else if (shiftLower.includes("check out ca tối") || shiftLower.includes("check out ca toi")) {
      dayData.evening.out = timeStr;
    } else {
      const shiftTypeDebug = row[actualShiftCol];
      const shiftTypeDebugStr = shiftTypeDebug instanceof Date ? shiftTypeDebug.toString() : String(shiftTypeDebug || "").trim();
      Logger.log(`   WARNING: Row ${r + 1}, emp=${empCode}, date=${dateKey}: Unknown shift type: "${shiftType}" (raw value: "${shiftTypeDebugStr}", column ${actualShiftCol})`);
    }
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

  // Header CB–DF: offset theo chu kỳ (29-ngoài … 28-ngoài), cùng logic với DO–ES
  const masterInfo = buildMasterInfo_(masterSh, MASTER_EMP_COL, MASTER_HEADER_ROW, null, null);
  const { empToRow, colByDay } = masterInfo;

  // ====== 4) BUILD DATA TO WRITE ======
  Logger.log("4) Building data to write (columns CB-DF, " + OFFLINE_START_COL + "-" + OFFLINE_END_COL + ")...");
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

    for (const [dateKey, dayData] of dayMap.entries()) {
      const parts = dateKey.split("-");
      if (parts.length !== 3) {
        Logger.log(`   WARNING: Invalid dateKey "${dateKey}"`);
        continue;
      }
      const py = parseInt(parts[0], 10), pm = parseInt(parts[1], 10), pd = parseInt(parts[2], 10);
      const offlineC0 = payCycleColumnOffset_(py, pm, pd);

      if (offlineC0 < 0 || offlineC0 >= dayColsCount) {
        Logger.log(`   WARNING: date ${dateKey} offset ${offlineC0} is outside range [0, ${dayColsCount - 1}]`);
        continue;
      }

      // Validate: r0 phải trong phạm vi offlineBlock
      if (r0 < 0 || r0 >= dataRowCount) {
        Logger.log(`   WARNING: Row index ${r0} (row ${row1}) is outside range [0, ${dataRowCount - 1}]`);
        continue;
      }

      const targetCol = OFFLINE_START_COL + offlineC0;

      const hasMorning = dayData.morning && (dayData.morning.in || dayData.morning.out);
      const hasAfternoon = dayData.afternoon && (dayData.afternoon.in || dayData.afternoon.out);
      const hasEvening = dayData.evening && (dayData.evening.in || dayData.evening.out);
      const segments = [];

      if (hasMorning && hasAfternoon && !hasEvening) {
        const morningTimes = [];
        if (dayData.morning.in) morningTimes.push(dayData.morning.in);
        if (dayData.morning.out) morningTimes.push(dayData.morning.out);
        const afternoonTimes = [];
        if (dayData.afternoon.in) afternoonTimes.push(dayData.afternoon.in);
        if (dayData.afternoon.out) afternoonTimes.push(dayData.afternoon.out);
        segments.push([...morningTimes, ...afternoonTimes].join("\n") + "\noff 2 ca");
      } else {
        if (hasMorning) {
          const morningTimes = [];
          if (dayData.morning.in) morningTimes.push(dayData.morning.in);
          if (dayData.morning.out) morningTimes.push(dayData.morning.out);
          segments.push(morningTimes.join("\n") + "\noff ca sáng");
        }
        if (hasAfternoon) {
          const afternoonTimes = [];
          if (dayData.afternoon.in) afternoonTimes.push(dayData.afternoon.in);
          if (dayData.afternoon.out) afternoonTimes.push(dayData.afternoon.out);
          segments.push(afternoonTimes.join("\n") + "\noff ca chiều");
        }
      }
      if (hasEvening) {
        const eveningTimes = [];
        if (dayData.evening.in) eveningTimes.push(dayData.evening.in);
        if (dayData.evening.out) eveningTimes.push(dayData.evening.out);
        segments.push(eveningTimes.join("\n") + "\noff ca tối");
      }
      if (segments.length) offlineBlock[r0][offlineC0] = segments.join("\n");

      if (offlineBlock[r0][offlineC0]) {
        updatedCells++;
        // Debug: Log vài cell đầu để kiểm tra mapping
        if (updatedCells <= 5) {
          Logger.log(`   DEBUG: Writing date ${dateKey} (col ${targetCol}, offset ${offlineC0}) for emp ${empCode} at row ${row1}`);
        }
      }
    }
  }

  Logger.log(`   Prepared ${updatedCells} cells to update`);

  // ====== BÁO CÁO MÃ KHÔNG IMPORT ĐƯỢC ======
  if (notFound.length > 0) {
    Logger.log(`⚠ CẢNH BÁO: ${notFound.length} mã có data OFFLINE nhưng KHÔNG TÌM THẤY trong sheet tổng:`);
    notFound.forEach((code, i) => Logger.log(`   ${i + 1}. ${code}`));
  } else {
    Logger.log(`✓ Tất cả mã tìm thấy trong sheet tổng.`);
  }
  if (skippedEmpCodes.size > 0) {
    Logger.log(`⚠ ${skippedEmpCodes.size} mã bị bỏ qua vì không hợp lệ (ví dụ: #N/A, trống): ${Array.from(skippedEmpCodes).join(", ")}`);
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
    let message = `✓ Import OFFLINE: ${updatedCells} ô vào cột CB-DF`;
    if (notFound.length > 0) message += ` | ⚠ ${notFound.length} mã không có trong sheet tổng: ${notFound.slice(0, 5).join(", ")}${notFound.length > 5 ? '...' : ''}`;
    if (skippedEmpCodes.size > 0) message += ` | ${skippedEmpCodes.size} mã không hợp lệ`;
    if (errorCount > 0) message += ` | ${errorCount} batch lỗi`;
    SpreadsheetApp.getActiveSpreadsheet().toast(message, "Import Offline Form", 10);
    Logger.log(`Toast: ${message}`);
  } catch (e) {
    Logger.log(`Finished: Updated ${updatedCells} ô. notFound=${notFound.length}, skippedEmp=${skippedEmpCodes.size}`);
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
      .addItem('Tổng ca tháng (BU)', 'UI_totalCaThang')
      .addSeparator()
      .addItem('Highlight Lỗi trễ và quên check in out tự động', 'UI_highlightError')
  );

  menu.addSubMenu(
    ui.createMenu('ONL (Form)')
      .addItem('Import data chấm công ONL form (DO-ES)', 'UI_importOnlForm')
      .addSeparator()
      .addItem('Tổng ca ONL form (EX)', 'UI_totalCaOnlForm')
  );

  menu.addSubMenu(
    ui.createMenu('OFF ngoài (Form)')
      .addItem('Import data chấm công OFF ngoài form (CB-DF)', 'UI_importOffNgoaiForm')
      .addSeparator()
      .addItem('Tổng ca OFF form (DL)', 'UI_totalCaOffForm')
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

function UI_totalCaOnlForm() {
  if (!UI_confirmRun_('Tổng ca ONL form (EX)', 'Sẽ cập nhật cột EX = tổng ca ONL form (DO-ES).')) return;
  return UI_run_('Tổng ca ONL form (EX)', () => updateTongCaOnlFormCommit());
}

// -------- OFF ngoài (Form) --------
function UI_importOffNgoaiForm() {
  if (!UI_confirmRun_('Import OFF ngoài form', 'Sẽ import dữ liệu chấm công OFF ngoài từ form vào cột CB-DF.')) return;
  return UI_run_('Import OFF ngoài form', () => importOfflineFormToMaster());
}

function UI_totalCaOffForm() {
  if (!UI_confirmRun_('Tổng ca OFF form (DL)', 'Sẽ cập nhật cột DL = tổng ca OFF ngoài form (CB-DF).')) return;
  return UI_run_('Tổng ca OFF form (DL)', () => updateTongCaOffFormCommit());
}