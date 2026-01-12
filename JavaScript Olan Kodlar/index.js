<<<<<<< HEAD
const { onValueWritten } = require("firebase-functions/v2/database");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");

if (!admin.apps.length) {
  admin.initializeApp();
}
const db = admin.database();

function getTRTime() {
  const now = new Date();
  const trTime = new Date(now.getTime() + (3 * 60 * 60 * 1000));

  let day = trTime.getUTCDay() - 1;
  if (day === -1) day = 6; // 0=Pazartesi, 6=Pazar

  const hour = trTime.getUTCHours();
  const timestamp = Math.floor(now.getTime() / 1000);

  return { day, hour, timestamp };
}

async function updateDataset(uid) {
  try {
    const [kontrolSnap, homeSnap] = await Promise.all([
      db.ref(`/users/${uid}/Kontrol`).once("value"),
      db.ref(`/users/${uid}/ai/home_away`).once("value")
    ]);

    if (!kontrolSnap.exists()) return;

    const kontrol = kontrolSnap.val();
    const termostat = kontrol.termostat || {};
    const homeStatus = (homeSnap.exists() && homeSnap.val().status === "HOME") ? 1 : 0;
    const timeData = getTRTime();

    const row = {
      day: timeData.day,
      hour: timeData.hour,
      home: homeStatus,
      currentTemp: termostat.mevcutderece ?? null,
      humidity: termostat.nem ?? null,
      targetTemp: termostat.hedefderece ?? null,
      kombiOn: termostat.kombionoff ? 1 : 0,
      kombiDurum: termostat.kombiDurum ? 1 : 0,
      eko: termostat.ekomod ? 1 : 0,
      device1: kontrol.cihaz1?.durum ? 1 : 0,
      device2: kontrol.cihaz2?.durum ? 1 : 0,
      device3: kontrol.cihaz3?.durum ? 1 : 0,
      device4: kontrol.cihaz4?.durum ? 1 : 0,
      sourceTimestamp: timeData.timestamp.toString(),
      createdAt: admin.database.ServerValue.TIMESTAMP
    };

    const datasetRef = db.ref(`/users/${uid}/ai/dataset`);
    const recentSnap = await datasetRef.orderByChild("sourceTimestamp").limitToLast(20).once("value");

    let foundKey = null;
    const fiveMinAgo = timeData.timestamp - (5 * 60);

    if (recentSnap.exists()) {
      const records = recentSnap.val();
      const keys = Object.keys(records).reverse();
      for (const key of keys) {
        if (parseInt(records[key].sourceTimestamp || 0) >= fiveMinAgo) {
          foundKey = key;
          break;
        }
      }
    }

    if (foundKey) {
      await datasetRef.child(foundKey).update(row);
      logger.debug(`📝 Dataset güncellendi (${uid})`);
    } else {
      await datasetRef.push(row);
      logger.debug(`➕ Yeni dataset satırı (${uid})`);
    }
  } catch (error) {
    logger.error("UpdateDataset hatası:", error);
  }
}


exports.konumDegisikligiTakip = onValueWritten({
  ref: "users/{userId}/ai/home_away",
  region: "us-central1"
}, async (event) => {
  const userId = event.params.userId;
  const newData = event.data.after.val();
  const oldData = event.data.before.val();

  if (!newData || newData.status === (oldData ? oldData.status : null)) return;

  const newStatus = newData.status;
  const triggerType = newStatus === "HOME" ? "arrive" : (newStatus === "AWAY" ? "leave" : null);

  if (!triggerType) return;

  logger.info(`📍 ${userId}: Konum değişti -> ${newStatus}`);

  try {
    const programsSnap = await db.ref(`users/${userId}/Program`).once("value");
    const programs = programsSnap.val();
    if (!programs) return;

    const promises = [];
    let islemYapildi = false;

    Object.values(programs).forEach((progData) => {
      if (!progData || !progData.enabled || progData.trigger !== triggerType) return;

      islemYapildi = true;
      logger.info(`🚀 Program Tetiklendi: ${triggerType}`);

      // Cihazları yönet
      if (progData.devices) {
        Object.entries(progData.devices).forEach(([dev, state]) => {
          promises.push(db.ref(`users/${userId}/Kontrol/${dev}/durum`).set(state));
        });
      }
      // Termostatı yönet
      const targetTemp = progData.target_temp !== undefined ? progData.target_temp : progData.hedefderece;

      if (targetTemp !== undefined) {
        // kombionoff varsa onu kullan, yoksa ve "arrive" ise aç (true), "leave" ise kapatma (false) değil, kullanıcı ayarına bırak?
        const kombiState = progData.kombionoff !== undefined ? progData.kombionoff : (triggerType === "arrive");

        logger.info(`🌡️ Termostat Güncelleniyor: ${targetTemp}°C (Kombi: ${kombiState})`);

        promises.push(db.ref(`users/${userId}/Kontrol/termostat`).update({
          hedefderece: parseFloat(targetTemp),
          kombionoff: kombiState
        }));
      }
    });

    await Promise.all(promises);
    if (islemYapildi) {
      await db.ref(`users/${userId}/ai/home_away/last_program_trigger_time`).set(Math.floor(Date.now() / 1000));
    }

  } catch (error) {
    logger.error(`❌ Konum hatası:`, error);
  }
});


exports.buildAIDatasetTermostat = onValueWritten({
  ref: "users/{uid}/Kontrol/termostat",
  region: "us-central1"
}, async (event) => {
  const uid = event.params.uid;
  const after = event.data.after.val();
  const before = event.data.before.val();

  if (!after) return;
  if (before) {
    const isChange =
      after.hedefderece !== before.hedefderece ||
      after.kombionoff !== before.kombionoff ||
      Math.abs((after.mevcutderece || 0) - (before.mevcutderece || 0)) > 0.4;

    if (!isChange) return;
  }

  await updateDataset(uid);
});

exports.buildAIDatasetCihazlar = onValueWritten({
  ref: "users/{uid}/Kontrol/{deviceId}/durum",
  region: "us-central1"
}, async (event) => {
  const uid = event.params.uid;
  const deviceId = event.params.deviceId;

  const after = event.data.after.val();
  const before = event.data.before.val();
  if (!deviceId.startsWith("cihaz")) return;
  if (after === before) return;

  logger.debug(`🔔 ${deviceId} durumu değişti`);
  await updateDataset(uid);
});
=======
const { onValueWritten } = require("firebase-functions/v2/database");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");

if (!admin.apps.length) {
  admin.initializeApp();
}
const db = admin.database();

function getTRTime() {
  const now = new Date();
  const trTime = new Date(now.getTime() + (3 * 60 * 60 * 1000));

  let day = trTime.getUTCDay() - 1;
  if (day === -1) day = 6; // 0=Pazartesi, 6=Pazar

  const hour = trTime.getUTCHours();
  const timestamp = Math.floor(now.getTime() / 1000);

  return { day, hour, timestamp };
}

async function updateDataset(uid) {
  try {
    const [kontrolSnap, homeSnap] = await Promise.all([
      db.ref(`/users/${uid}/Kontrol`).once("value"),
      db.ref(`/users/${uid}/ai/home_away`).once("value")
    ]);

    if (!kontrolSnap.exists()) return;

    const kontrol = kontrolSnap.val();
    const termostat = kontrol.termostat || {};
    const homeStatus = (homeSnap.exists() && homeSnap.val().status === "HOME") ? 1 : 0;
    const timeData = getTRTime();

    const row = {
      day: timeData.day,
      hour: timeData.hour,
      home: homeStatus,
      currentTemp: termostat.mevcutderece ?? null,
      humidity: termostat.nem ?? null,
      targetTemp: termostat.hedefderece ?? null,
      kombiOn: termostat.kombionoff ? 1 : 0,
      kombiDurum: termostat.kombiDurum ? 1 : 0,
      eko: termostat.ekomod ? 1 : 0,
      device1: kontrol.cihaz1?.durum ? 1 : 0,
      device2: kontrol.cihaz2?.durum ? 1 : 0,
      device3: kontrol.cihaz3?.durum ? 1 : 0,
      device4: kontrol.cihaz4?.durum ? 1 : 0,
      sourceTimestamp: timeData.timestamp.toString(),
      createdAt: admin.database.ServerValue.TIMESTAMP
    };

    const datasetRef = db.ref(`/users/${uid}/ai/dataset`);
    const recentSnap = await datasetRef.orderByChild("sourceTimestamp").limitToLast(20).once("value");

    let foundKey = null;
    const fiveMinAgo = timeData.timestamp - (5 * 60);

    if (recentSnap.exists()) {
      const records = recentSnap.val();
      const keys = Object.keys(records).reverse();
      for (const key of keys) {
        if (parseInt(records[key].sourceTimestamp || 0) >= fiveMinAgo) {
          foundKey = key;
          break;
        }
      }
    }

    if (foundKey) {
      await datasetRef.child(foundKey).update(row);
      logger.debug(`📝 Dataset güncellendi (${uid})`);
    } else {
      await datasetRef.push(row);
      logger.debug(`➕ Yeni dataset satırı (${uid})`);
    }
  } catch (error) {
    logger.error("UpdateDataset hatası:", error);
  }
}


exports.konumDegisikligiTakip = onValueWritten({
  ref: "users/{userId}/ai/home_away",
  region: "us-central1"
}, async (event) => {
  const userId = event.params.userId;
  const newData = event.data.after.val();
  const oldData = event.data.before.val();

  if (!newData || newData.status === (oldData ? oldData.status : null)) return;

  const newStatus = newData.status;
  const triggerType = newStatus === "HOME" ? "arrive" : (newStatus === "AWAY" ? "leave" : null);

  if (!triggerType) return;

  logger.info(`📍 ${userId}: Konum değişti -> ${newStatus}`);

  try {
    const programsSnap = await db.ref(`users/${userId}/Program`).once("value");
    const programs = programsSnap.val();
    if (!programs) return;

    const promises = [];
    let islemYapildi = false;

    Object.values(programs).forEach((progData) => {
      if (!progData || !progData.enabled || progData.trigger !== triggerType) return;

      islemYapildi = true;
      logger.info(`🚀 Program Tetiklendi: ${triggerType}`);

      // Cihazları yönet
      if (progData.devices) {
        Object.entries(progData.devices).forEach(([dev, state]) => {
          promises.push(db.ref(`users/${userId}/Kontrol/${dev}/durum`).set(state));
        });
      }
      // Termostatı yönet
      const targetTemp = progData.target_temp !== undefined ? progData.target_temp : progData.hedefderece;

      if (targetTemp !== undefined) {
        // kombionoff varsa onu kullan, yoksa ve "arrive" ise aç (true), "leave" ise kapatma (false) değil, kullanıcı ayarına bırak?
        const kombiState = progData.kombionoff !== undefined ? progData.kombionoff : (triggerType === "arrive");

        logger.info(`🌡️ Termostat Güncelleniyor: ${targetTemp}°C (Kombi: ${kombiState})`);

        promises.push(db.ref(`users/${userId}/Kontrol/termostat`).update({
          hedefderece: parseFloat(targetTemp),
          kombionoff: kombiState
        }));
      }
    });

    await Promise.all(promises);
    if (islemYapildi) {
      await db.ref(`users/${userId}/ai/home_away/last_program_trigger_time`).set(Math.floor(Date.now() / 1000));
    }

  } catch (error) {
    logger.error(`❌ Konum hatası:`, error);
  }
});


exports.buildAIDatasetTermostat = onValueWritten({
  ref: "users/{uid}/Kontrol/termostat",
  region: "us-central1"
}, async (event) => {
  const uid = event.params.uid;
  const after = event.data.after.val();
  const before = event.data.before.val();

  if (!after) return;
  if (before) {
    const isChange =
      after.hedefderece !== before.hedefderece ||
      after.kombionoff !== before.kombionoff ||
      Math.abs((after.mevcutderece || 0) - (before.mevcutderece || 0)) > 0.4;

    if (!isChange) return;
  }

  await updateDataset(uid);
});

exports.buildAIDatasetCihazlar = onValueWritten({
  ref: "users/{uid}/Kontrol/{deviceId}/durum",
  region: "us-central1"
}, async (event) => {
  const uid = event.params.uid;
  const deviceId = event.params.deviceId;

  const after = event.data.after.val();
  const before = event.data.before.val();
  if (!deviceId.startsWith("cihaz")) return;
  if (after === before) return;

  logger.debug(`🔔 ${deviceId} durumu değişti`);
  await updateDataset(uid);
});
>>>>>>> 2c4889efa6ef6da213f432eb60fe86a755546eda
