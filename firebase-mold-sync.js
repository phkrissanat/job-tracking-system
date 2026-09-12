import { doc, setDoc, getDoc, deleteDoc, onSnapshot, collection, addDoc, collectionGroup, query, where, getDocs, runTransaction } from "https://www.gstatic.com/firebasejs/10.13.2/firebase-firestore.js";
import { signInAnonymously } from "https://www.gstatic.com/firebasejs/10.13.2/firebase-auth.js";
import { db, auth } from "./firebase-init.js";

// Firestore caps a single document at ~1MiB. We stay well under that per
// chunk to leave headroom for field-name/BSON-style overhead, and because
// JS string .length counts UTF-16 code units, not the UTF-8 bytes Firestore
// actually bills against (Thai text is ~3 bytes/char) — so we measure real
// byte size with TextEncoder rather than trusting .length.
const CHUNK_BYTE_BUDGET = 700000;
const enc = new TextEncoder();
function byteLen(v){ return enc.encode(JSON.stringify(v)).length; }

function buildChunks(rows){
  var chunks=[], current=[];
  for(var i=0;i<rows.length;i++){
    current.push(rows[i]);
    if(byteLen(current) > CHUNK_BYTE_BUDGET){
      var last=current.pop();
      if(current.length) chunks.push(current);
      current=[last];
    }
  }
  if(current.length) chunks.push(current);
  return chunks;
}

// Generic chunked push/clear/load against a given Firestore collection name.
// Both "งานปัจจุบัน" (mold_sync) and "ทะเบียนแม่พิมพ์หลัก" (mold_registry_sync)
// share this exact logic — only the collection name differs — so it's kept
// as one parameterized implementation instead of copy-pasted per dataset.
async function pushToCollection(collectionName, rows, srcName){
  var metaSnap = await getDoc(doc(db,collectionName,'meta'));
  var oldChunkCount = metaSnap.exists() ? (metaSnap.data().chunkCount||0) : 0;
  var chunks = buildChunks(rows);
  var writes = chunks.map(function(chunk,i){
    return setDoc(doc(db,collectionName,'chunk_'+i), { rows: chunk });
  });
  await Promise.all(writes);
  await setDoc(doc(db,collectionName,'meta'), {
    chunkCount: chunks.length,
    totalCount: rows.length,
    ts: Date.now(),
    src: srcName || ''
  });
  var cleanup=[];
  for(var j=chunks.length;j<oldChunkCount;j++){
    cleanup.push(deleteDoc(doc(db,collectionName,'chunk_'+j)));
  }
  await Promise.all(cleanup);
  return { chunkCount: chunks.length, totalCount: rows.length };
}
async function clearCollection(collectionName){
  var metaSnap = await getDoc(doc(db,collectionName,'meta'));
  var oldChunkCount = metaSnap.exists() ? (metaSnap.data().chunkCount||0) : 0;
  var cleanup=[];
  for(var j=0;j<oldChunkCount;j++){
    cleanup.push(deleteDoc(doc(db,collectionName,'chunk_'+j)));
  }
  await Promise.all(cleanup);
  await setDoc(doc(db,collectionName,'meta'), { chunkCount:0, totalCount:0, ts:Date.now(), src:'' });
}
async function loadFromCollection(collectionName){
  var metaSnap = await getDoc(doc(db,collectionName,'meta'));
  if(!metaSnap.exists()) return { data:[], ts:null, src:'' };
  var meta = metaSnap.data();
  var chunkCount = meta.chunkCount||0;
  if(!chunkCount) return { data:[], ts:meta.ts||null, src:meta.src||'' };
  var reads=[];
  for(var i=0;i<chunkCount;i++){
    reads.push(getDoc(doc(db,collectionName,'chunk_'+i)));
  }
  var snaps = await Promise.all(reads);
  var data=[];
  snaps.forEach(function(s){ if(s.exists()) data=data.concat(s.data().rows||[]); });
  return { data:data, ts:meta.ts||null, src:meta.src||'' };
}

// ── งานปัจจุบัน (current jobs)
export function fbPushMold(rows, srcName){ return pushToCollection('mold_sync', rows, srcName); }
export function fbClearMold(){ return clearCollection('mold_sync'); }
export function fbLoadMold(){ return loadFromCollection('mold_sync'); }

// ── ทะเบียนแม่พิมพ์หลัก (master mold registry)
export function fbPushRegistry(rows, srcName){ return pushToCollection('mold_registry_sync', rows, srcName); }
export function fbClearRegistry(){ return clearCollection('mold_registry_sync'); }
export function fbLoadRegistry(){ return loadFromCollection('mold_registry_sync'); }

// ── AUTO SYNC (real-time listeners for readonly.html)
// Watches the 'meta' doc of a collection; whenever admin pushes/clears data,
// meta changes and we re-read all chunks fresh, then hand the full dataset
// to the callback. Returns an unsubscribe function — call it to stop
// listening (e.g. when the page unloads).
function watchCollection(collectionName, callback){
  return onSnapshot(doc(db, collectionName, 'meta'), async function(metaSnap){
    if(!metaSnap.exists()){ callback({ data: [], ts: null, src: '' }); return; }
    var meta = metaSnap.data();
    var chunkCount = meta.chunkCount || 0;
    if(!chunkCount){ callback({ data: [], ts: meta.ts||null, src: meta.src||'' }); return; }
    try{
      var reads = [];
      for(var i=0; i<chunkCount; i++){ reads.push(getDoc(doc(db, collectionName, 'chunk_'+i))); }
      var snaps = await Promise.all(reads);
      var data = [];
      snaps.forEach(function(s){ if(s.exists()) data = data.concat(s.data().rows || []); });
      callback({ data: data, ts: meta.ts||null, src: meta.src||'' });
    }catch(err){
      callback({ error: err });
    }
  }, function(err){
    callback({ error: err });
  });
}
export function fbWatchMold(callback){ return watchCollection('mold_sync', callback); }
export function fbWatchRegistry(callback){ return watchCollection('mold_registry_sync', callback); }

// ── COLUMN WIDTH SETTINGS (admin-adjustable, live-synced to readonly)
// A single small doc — not chunked, this is just a handful of numbers.
export async function fbPushColumnWidths(widths){
  await setDoc(doc(db,'app_settings','column_widths'), { widths: widths, ts: Date.now() });
}
export function fbWatchColumnWidths(callback){
  return onSnapshot(doc(db,'app_settings','column_widths'), function(snap){
    if(!snap.exists()){ callback({ widths: null }); return; }
    var d=snap.data();
    callback({ widths: d.widths||null, ts: d.ts||null });
  }, function(err){
    callback({ error: err });
  });
}

// ── SUB-USER ACCOUNTS (Validator / Data Specialist)
// Not real Firebase Auth accounts — there's no backend here to run the
// Admin SDK's user-creation, so these are lightweight username/password
// records the signed-in admin creates from their own dashboard, stored
// under the admin's own UID (admins/{adminUID}/validators|dataSpecialists).
// This matches the pattern already used for mold-building-job-tracker's
// role accounts. Fine for a low-risk internal tool; not real security —
// passwords sit in Firestore in plain text, readable by anything with a
// valid credential match, not encrypted the way Firebase Auth handles it.
function subUserCollName(role){
  if(role!=='validators' && role!=='dataSpecialists') throw new Error('invalid role: '+role);
  return role;
}

export async function fbCreateSubUser(role, username, password, name){
  if(!auth.currentUser) throw new Error('ต้องเข้าสู่ระบบ admin ก่อน');
  var col = subUserCollName(role);
  var ref = collection(db, 'admins', auth.currentUser.uid, col);
  await addDoc(ref, {
    username: String(username).trim(),
    password: String(password),
    name: name ? String(name).trim() : String(username).trim(),
    active: true,
    createdAt: Date.now()
  });
}

// Live list for the admin's own account-management screen — scoped to the
// signed-in admin's own subcollection only, so one admin never sees or
// touches another admin's accounts even if this app grows multi-admin.
export function fbWatchSubUsers(role, callback){
  if(!auth.currentUser){ callback({ error: new Error('ต้องเข้าสู่ระบบ admin ก่อน') }); return function(){}; }
  var col = subUserCollName(role);
  var ref = collection(db, 'admins', auth.currentUser.uid, col);
  return onSnapshot(ref, function(snap){
    var list=[];
    snap.forEach(function(d){ list.push(Object.assign({id:d.id}, d.data())); });
    list.sort(function(a,b){return (b.createdAt||0)-(a.createdAt||0);});
    callback({ list: list });
  }, function(err){
    callback({ error: err });
  });
}

export async function fbDeleteSubUser(role, id){
  if(!auth.currentUser) throw new Error('ต้องเข้าสู่ระบบ admin ก่อน');
  var col = subUserCollName(role);
  await deleteDoc(doc(db, 'admins', auth.currentUser.uid, col, id));
}

export async function fbSetSubUserActive(role, id, active){
  if(!auth.currentUser) throw new Error('ต้องเข้าสู่ระบบ admin ก่อน');
  var col = subUserCollName(role);
  await setDoc(doc(db, 'admins', auth.currentUser.uid, col, id), { active: !!active }, { merge: true });
}

// Public login check — used by the login box on readonly.html (and by
// validator.html/data-specialist.html directly if opened without going
// through that box first). Queries across every admin's subcollection via
// a Firestore collection-group query, filtered on username only: a
// two-field equality filter on a collection-group query needs a composite
// index, and this keeps setup to the simpler single-field collection-group
// index instead. Password is compared client-side once the candidate
// doc(s) come back, since usernames are expected to be unique anyway.
export async function fbCheckSubUserLogin(username, password){
  var uname = String(username||'').trim();
  var pass = String(password||'');
  if(!uname || !pass) return null;
  var roles = [
    { col: 'validators', role: 'validator' },
    { col: 'dataSpecialists', role: 'dataSpecialist' }
  ];
  for (var i=0; i<roles.length; i++){
    var r = roles[i];
    var q = query(collectionGroup(db, r.col), where('username','==',uname));
    var snap;
    try{ snap = await getDocs(q); }catch(err){ return { error: err }; }
    var found = null;
    snap.forEach(function(d){
      var data = d.data();
      if(!found && data.password===pass && data.active!==false){
        found = { role: r.role, id: d.id, adminId: d.ref.parent.parent.id, name: data.name||data.username };
      }
    });
    if(found) return found;
  }
  return null;
}

// Full login used by validator.html / data-specialist.html / readonly.html's
// login box: checks the credential, then — only on a match — signs in
// anonymously so Firestore Rules see a real request.auth for the write
// that follows (submitting a trial result, saving an edited row). This
// keeps mold_sync/trial_history writes gated behind request.auth != null
// the same way admin's already are, instead of opening writes to anyone.
// The anonymous session carries no identity of its own — the matched
// name/role from the credential check is what the calling page should
// keep and display, not anything from the anon auth user.
export async function fbSubUserLogin(username, password){
  var result = await fbCheckSubUserLogin(username, password);
  if(!result || result.error) return result;
  try{
    await signInAnonymously(auth);
  }catch(err){
    return { error: err };
  }
  return result;
}

// ── TRIAL HISTORY (Validator submissions)
// Each item's trial rounds are kept in their own doc, one per รหัสชิ้นงาน
// (code) — {rounds: [{round, date, result, validatedBy, ts}, ...]}. The
// main jobs table always shows only the latest round (TRIAL ครั้งที่/วันที่/
// ผล TRIAL), which this same function keeps in sync on every submission.
//
// Both the trial-history doc and the jobs chunk that holds the matching
// row are read and written inside one Firestore transaction, so two
// validators submitting for different codes at the same moment (or a
// validator submitting while an admin re-uploads) can't silently clobber
// each other — Firestore retries the whole transaction if anything it
// read changes before the commit.
function sanitizeDocId(s){ return String(s).replace(/[\/]/g,'_').slice(0,300) || '_'; }

export async function fbGetTrialHistory(code){
  var id = sanitizeDocId(code);
  var snap = await getDoc(doc(db,'trial_history',id));
  return snap.exists() ? (snap.data().rounds||[]) : [];
}

export async function fbSubmitTrialResult(code, resultValue, validatorName){
  var codeStr = String(code||'').trim();
  if(!codeStr) throw new Error('ไม่มีรหัสชิ้นงาน — ส่งผลไม่ได้');
  var historyId = sanitizeDocId(codeStr);
  var historyRef = doc(db,'trial_history',historyId);
  var metaRef = doc(db,'mold_sync','meta');

  return runTransaction(db, async function(tx){
    var metaSnap = await tx.get(metaRef);
    if(!metaSnap.exists()) throw new Error('ยังไม่มีข้อมูลงานในระบบ');
    var chunkCount = metaSnap.data().chunkCount || 0;

    // Read every chunk inside the transaction (Firestore requires all
    // reads before any write in a transaction) to find which one holds
    // this code's row.
    var chunkRefs = [];
    for (var i=0; i<chunkCount; i++) chunkRefs.push(doc(db,'mold_sync','chunk_'+i));
    var chunkSnaps = [];
    for (var j=0; j<chunkRefs.length; j++) chunkSnaps.push(await tx.get(chunkRefs[j]));

    var targetChunkIdx = -1, targetRowIdx = -1, rows = null;
    for (var c=0; c<chunkSnaps.length; c++){
      if(!chunkSnaps[c].exists()) continue;
      var r = chunkSnaps[c].data().rows || [];
      var idx = r.findIndex(function(row){ return (row.code||'').trim()===codeStr; });
      if(idx>=0){ targetChunkIdx=c; targetRowIdx=idx; rows=r; break; }
    }
    if(targetChunkIdx<0) throw new Error('ไม่พบงานที่มีรหัสชิ้นงานนี้ในระบบ (อาจถูกอัปโหลดทับไปแล้ว)');

    var historySnap = await tx.get(historyRef);
    var existingRounds = historySnap.exists() ? (historySnap.data().rounds||[]) : [];
    var maxRound = existingRounds.reduce(function(m,r){return Math.max(m,r.round||0);},0);
    var newRound = maxRound+1;

    // Require the ISO inspection checklist (form 740-FM-028) to be filled
    // in and marked complete for this exact round before a TRIAL result
    // can be submitted — matches how this is done on paper today.
    var checklistRef = doc(db,'trial_checklists', sanitizeDocId(codeStr)+'_'+newRound);
    var checklistSnap = await tx.get(checklistRef);
    if(!checklistSnap.exists() || !checklistSnap.data().completed){
      throw new Error('ต้องกรอกแบบฟอร์มตรวจสอบ TRIAL ให้ครบทุกจุดก่อน จึงจะส่งผลได้');
    }

    var todayStr = new Date().toLocaleDateString('en-GB').split('/').join('/'); // DD/MM/YYYY, matches existing data
    var resultNorm = resultValue==='ok' ? 'ok' : 'ng';

    var entry = { round:newRound, date:todayStr, result:resultNorm, validatedBy:validatorName||'', ts:Date.now() };
    var newRounds = existingRounds.concat([entry]);

    rows[targetRowIdx] = Object.assign({}, rows[targetRowIdx], {
      trialRound: String(newRound),
      trialDate: todayStr,
      result: resultNorm
    });

    tx.set(chunkRefs[targetChunkIdx], { rows: rows });
    tx.set(historyRef, { rounds: newRounds, code: codeStr, updatedAt: Date.now() });

    return entry;
  });
}

// ── ROW EDIT (Data Specialist)
// Locates the target row by matching its full original snapshot against
// the freshest data at write time (not by array index, which can silently
// point at the wrong row if anything else changed the dataset between the
// edit form opening and Save). If the row can't be found — someone else
// edited or re-uploaded in the meantime — this throws rather than guessing,
// and the caller should ask the person to refresh and retry.
export async function fbUpdateJobRow(originalRow, updatedRow){
  var metaRef = doc(db,'mold_sync','meta');
  var fingerprint = JSON.stringify(originalRow);

  return runTransaction(db, async function(tx){
    var metaSnap = await tx.get(metaRef);
    if(!metaSnap.exists()) throw new Error('ยังไม่มีข้อมูลงานในระบบ');
    var chunkCount = metaSnap.data().chunkCount || 0;

    var chunkRefs = [];
    for (var i=0; i<chunkCount; i++) chunkRefs.push(doc(db,'mold_sync','chunk_'+i));
    var chunkSnaps = [];
    for (var j=0; j<chunkRefs.length; j++) chunkSnaps.push(await tx.get(chunkRefs[j]));

    for (var c=0; c<chunkSnaps.length; c++){
      if(!chunkSnaps[c].exists()) continue;
      var rows = chunkSnaps[c].data().rows || [];
      var idx = rows.findIndex(function(row){ return JSON.stringify(row)===fingerprint; });
      if(idx>=0){
        rows[idx] = updatedRow;
        tx.set(chunkRefs[c], { rows: rows });
        return true;
      }
    }
    throw new Error('ไม่พบแถวนี้ในระบบแล้ว ข้อมูลอาจถูกแก้ไขหรืออัปโหลดทับไปแล้ว กรุณารีเฟรชแล้วลองใหม่');
  });
}

// ── TRIAL CHECKLIST (ISO inspection form, form 740-FM-028)
// One doc per code+round, since each TRIAL round gets its own filled-in
// checklist. Keyed the same way as trial_history so both are easy to
// cross-reference by (code, round).
function checklistDocId(code, round){ return sanitizeDocId(code)+'_'+round; }

export async function fbSaveTrialChecklist(code, round, data){
  var id = checklistDocId(code, round);
  await setDoc(doc(db,'trial_checklists',id), Object.assign({}, data, {
    code: code, round: round, updatedAt: Date.now()
  }));
}

export async function fbGetTrialChecklist(code, round){
  var id = checklistDocId(code, round);
  var snap = await getDoc(doc(db,'trial_checklists',id));
  return snap.exists() ? snap.data() : null;
}

// ── FROZEN COLUMNS (admin-adjustable, live-synced to readonly — same
// pattern as column widths: admin picks which columns stay pinned while
// scrolling the wide jobs table, readonly only ever displays what admin set)
export async function fbPushFreezeColumns(cks){
  await setDoc(doc(db,'app_settings','freeze_columns'), { cks: cks, ts: Date.now() });
}
export function fbWatchFreezeColumns(callback){
  return onSnapshot(doc(db,'app_settings','freeze_columns'), function(snap){
    if(!snap.exists()){ callback({ cks: null }); return; }
    var d=snap.data();
    callback({ cks: d.cks||null, ts: d.ts||null });
  }, function(err){
    callback({ error: err });
  });
}
