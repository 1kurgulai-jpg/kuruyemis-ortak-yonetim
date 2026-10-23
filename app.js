// ============================================================================
// KURUYEMİŞ ORTAK YÖNETİM & KÂR ANALİZ SİSTEMİ
// Gerçek Zamanlı Ortak Veri Senkronizasyonu (Firebase Realtime DB + BroadcastChannel + LocalStorage)
// ============================================================================

// VARSAYILAN GERÇEKÇİ KURUYEMİŞ DÜKKANI VERİLERİ
const DEFAULT_STORE_DATA = {
    roomPin: "1461",
    updatedAt: Date.now(),
    updatedBy: "Sistem",
    products: [
        {
            id: "p1",
            name: "Kavrulmuş Antep Fıstığı (Duble Ana Çıtlak)",
            category: "Kuruyemiş",
            unit: "kg",
            costPrice: 540,
            wastePercent: 2.0,
            targetPrice: 790,
            marketPrice: 820,
            dailySimQty: 6.5
        },
        {
            id: "p2",
            name: "Kavrulmuş Giresun Fındık İçi (Tombul)",
            category: "Kuruyemiş",
            unit: "kg",
            costPrice: 360,
            wastePercent: 1.5,
            targetPrice: 530,
            marketPrice: 550,
            dailySimQty: 8.0
        },
        {
            id: "p3",
            name: "Datça Çiğ Badem İçi (İri Boy)",
            category: "Kuruyemiş",
            unit: "kg",
            costPrice: 340,
            wastePercent: 1.0,
            targetPrice: 510,
            marketPrice: 500,
            dailySimQty: 5.5
        },
        {
            id: "p4",
            name: "Kavrulmuş Kaju (Jumbo W180)",
            category: "Kuruyemiş",
            unit: "kg",
            costPrice: 390,
            wastePercent: 2.0,
            targetPrice: 580,
            marketPrice: 600,
            dailySimQty: 7.0
        },
        {
            id: "p5",
            name: "Nevşehir Kabak Çekirdeği (Süt Kavrulmuş)",
            category: "Kuruyemiş",
            unit: "kg",
            costPrice: 210,
            wastePercent: 1.5,
            targetPrice: 330,
            marketPrice: 340,
            dailySimQty: 10.0
        },
        {
            id: "p6",
            name: "Tuzlu Dakota Ay Çekirdeği (Özel Kavrum)",
            category: "Kuruyemiş",
            unit: "kg",
            costPrice: 85,
            wastePercent: 2.5,
            targetPrice: 150,
            marketPrice: 160,
            dailySimQty: 25.0
        },
        {
            id: "p7",
            name: "Çifte Kavrulmuş Sarı Leblebi (Çorum)",
            category: "Kuruyemiş",
            unit: "kg",
            costPrice: 110,
            wastePercent: 1.0,
            targetPrice: 185,
            marketPrice: 190,
            dailySimQty: 12.0
        },
        {
            id: "p8",
            name: "Lüks Karışık Kuruyemiş (Özel Kokteyl)",
            category: "Kuruyemiş",
            unit: "kg",
            costPrice: 380,
            wastePercent: 1.5,
            targetPrice: 590,
            marketPrice: 610,
            dailySimQty: 11.0
        },
        {
            id: "p9",
            name: "Malatya Gün Kurusu Kayısı (Jumbo)",
            category: "Kuru Meyve",
            unit: "kg",
            costPrice: 240,
            wastePercent: 1.0,
            targetPrice: 370,
            marketPrice: 380,
            dailySimQty: 6.0
        },
        {
            id: "p10",
            name: "Aydın Dağ İnciri (Sarılop Kuru İncir)",
            category: "Kuru Meyve",
            unit: "kg",
            costPrice: 270,
            wastePercent: 1.5,
            targetPrice: 410,
            marketPrice: 420,
            dailySimQty: 4.5
        },
        {
            id: "p11",
            name: "Taze Çekilmiş Türk Kahvesi (Özel Harman)",
            category: "Kahve & Şekerleme",
            unit: "kg",
            costPrice: 310,
            wastePercent: 1.0,
            targetPrice: 480,
            marketPrice: 490,
            dailySimQty: 5.0
        },
        {
            id: "p12",
            name: "Antep Fıstıklı Çifte Kavrulmuş Lokum",
            category: "Kahve & Şekerleme",
            unit: "kg",
            costPrice: 220,
            wastePercent: 1.0,
            targetPrice: 360,
            marketPrice: 350,
            dailySimQty: 6.0
        }
    ],
    expenses: [
        { id: "e1", name: "Dükkan Kirası + Stopaj", amount: 35000 },
        { id: "e2", name: "Elektrik (Kavurma Fırını & Klima)", amount: 8500 },
        { id: "e3", name: "Kese Kağıdı, Ambalaj & Vakum Poşeti", amount: 4200 },
        { id: "e4", name: "Muhasebe, Vergi & POS Komisyon Gideri", amount: 5500 },
        { id: "e5", name: "Su, İnternet, Güvenlik & Temizlik", amount: 2800 },
        { id: "e6", name: "Part-Time Yardımcı Eleman / Sigorta", amount: 16000 }
    ],
    notes: [],
    dailySales: {},
    logs: [
        {
            id: "l1",
            author: "Enes & Ali Osman",
            action: "Kuruyemiş dükkanı ortak veri tabanı başlatıldı",
            time: Date.now() - 3600000 * 6
        }
    ]
};

// UYGULAMA DURUMU (STATE)
let currentPartner = localStorage.getItem("ky_partner_name") || "";
let currentRoom = localStorage.getItem("ky_room_code") || "bereket-kuruyemis";
let storeData = structuredClone(DEFAULT_STORE_DATA);
let activeTrackerDate = new Date().toISOString().slice(0, 10);

// Senkronizasyon Kanalları
let broadcastChannel = null;
let firebaseRestUrl = localStorage.getItem("ky_fb_url") || "";
let firebasePollingInterval = null;
let cloudEventSource = null;
let isApplyingRemoteUpdate = false;
const sessionClientId = "client_" + Math.random().toString(36).slice(2, 9);

// ============================================================================
// YARDIMCI HESAPLAMA VE FORMATLAMA FONKSİYONLARI
// ============================================================================

function formatTL(val) {
    const num = Number(val) || 0;
    return num.toLocaleString("tr-TR", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }) + " ₺";
}

function formatTLShort(val) {
    const num = Number(val) || 0;
    return Math.round(num).toLocaleString("tr-TR") + " ₺";
}

function formatQty(val, unit = "kg") {
    const num = Number(val) || 0;
    return num.toLocaleString("tr-TR", {
        minimumFractionDigits: 1,
        maximumFractionDigits: 1
    }) + " " + unit;
}

function formatTimeAgo(timestamp) {
    if (!timestamp) return "Az önce";
    const diffSec = Math.floor((Date.now() - timestamp) / 1000);
    if (diffSec < 15) return "Az önce";
    if (diffSec < 60) return `${diffSec} sn önce`;
    const diffMin = Math.floor(diffSec / 60);
    if (diffMin < 60) return `${diffMin} dk önce`;
    const diffHr = Math.floor(diffMin / 60);
    if (diffHr < 24) return `${diffHr} saat önce`;
    return new Date(timestamp).toLocaleDateString("tr-TR");
}

/**
 * Bir ürünün fire dahil gerçek maliyetini, birim net kârını ve brüt kâr marjını (%) hesaplar.
 * Brüt Kâr Marjı (%) = ((Satış Fiyatı - Fireli Maliyet) / Satış Fiyatı) * 100
 */
function calculateProductMetrics(product) {
    const cost = Number(product.costPrice) || 0;
    const waste = Number(product.wastePercent) || 0;
    const price = Number(product.targetPrice) || 0;
    const market = Number(product.marketPrice) || 0;
    const simQty = Number(product.dailySimQty) || 0;

    // Fire dahil net birim maliyet
    const realCost = cost * (1 + waste / 100);
    // Birim net kâr (TL)
    const unitProfit = price - realCost;
    // Brüt kâr marjı (%)
    const grossMargin = price > 0 ? (unitProfit / price) * 100 : 0;
    // Maliyet üzerine eklenen kâr oranı (Markup %)
    const markupPercent = realCost > 0 ? (unitProfit / realCost) * 100 : 0;

    // Piyasa kıyaslaması
    const marketDiff = price - market; // Negatifse piyasadan ucuz

    // Simülasyon değerleri
    const dailyRevenue = price * simQty;
    const dailyGrossProfit = unitProfit * simQty;
    const monthlyGrossProfit = dailyGrossProfit * 30;

    return {
        realCost,
        unitProfit,
        grossMargin,
        markupPercent,
        marketDiff,
        dailyRevenue,
        dailyGrossProfit,
        monthlyGrossProfit
    };
}

// ============================================================================
// GERÇEK ZAMANLI ORTAK SENKRONİZASYON MOTORU (LOCAL + BROADCAST + FIREBASE CLOUD)
// ============================================================================

function getStorageKey(room) {
    return `kuruyemis_ortak_oda_${room.trim().toLowerCase()}`;
}

function initSyncEngine(roomCode) {
    const cleanRoom = roomCode.trim().toLowerCase();

    // 1. Önce LocalStorage'dan mevcut oda verisini oku
    const savedRaw = localStorage.getItem(getStorageKey(cleanRoom));
    if (savedRaw) {
        try {
            storeData = JSON.parse(savedRaw);
            // Eski örnek notları (n1, n2) otomatik temizle
            if (Array.isArray(storeData.notes)) {
                storeData.notes = storeData.notes.filter((n) => n.id !== "n1" && n.id !== "n2");
            }
        } catch (err) {
            console.error("Veri okuma hatası:", err);
            storeData = structuredClone(DEFAULT_STORE_DATA);
        }
    } else {
        storeData = structuredClone(DEFAULT_STORE_DATA);
        localStorage.setItem(getStorageKey(cleanRoom), JSON.stringify(storeData));
    }

    // 2. Sekmeler arası anlık senkronizasyon (BroadcastChannel)
    if (broadcastChannel) {
        broadcastChannel.close();
    }
    if ("BroadcastChannel" in window) {
        broadcastChannel = new BroadcastChannel(`ky_channel_${cleanRoom}`);
        broadcastChannel.onmessage = (event) => {
            if (event.data && event.data.type === "SYNC_STATE") {
                applyIncomingData(event.data.payload, "Ortak Sekme");
            }
        };
    }

    // Storage event listener (farklı pencereler için yedek)
    window.onstorage = (e) => {
        if (e.key === getStorageKey(cleanRoom) && e.newValue) {
            try {
                const parsed = JSON.parse(e.newValue);
                applyIncomingData(parsed, "Ortak Pencere");
            } catch (err) {
                console.error(err);
            }
        }
    };

    // 3. Firebase Bulut Senkronizasyonu (Eğer özel URL tanımlanmışsa)
    setupFirebaseCloudSync(cleanRoom);

    // 4. Varsayılan Sıfır-Ayar Canlı Bulut Senkronizasyonu (Cihazlar ve Telefonlar Arası Anlık Eşitleme)
    setupInstantCloudSync(cleanRoom);

    updateSyncBadgeUI();
    renderAll();
}

function getCloudTopicName(roomCode) {
    const safeRoom = roomCode.replace(/[^a-z0-9_-]/gi, "_").toLowerCase();
    return `ky_ortak_db_v1_${safeRoom}`;
}

async function setupInstantCloudSync(roomCode) {
    if (cloudEventSource) {
        cloudEventSource.close();
        cloudEventSource = null;
    }

    const topic = getCloudTopicName(roomCode);

    // 1. Önce buluttaki son kaydedilmiş durumu çek
    try {
        const res = await fetch(`https://ntfy.sh/${topic}/json?poll=1&since=all`);
        if (res.ok) {
            const text = await res.text();
            const lines = text.trim().split("\n").filter(Boolean);
            for (let i = lines.length - 1; i >= 0; i--) {
                try {
                    const msg = JSON.parse(lines[i]);
                    if (msg.event === "message" && msg.message) {
                        const parsedPayload = JSON.parse(msg.message);
                        if (parsedPayload && parsedPayload.data && parsedPayload.data.updatedAt) {
                            if (parsedPayload.data.updatedAt > storeData.updatedAt) {
                                applyIncomingData(parsedPayload.data, "Bulut Senkronizasyon");
                            }
                            break;
                        }
                    }
                } catch (e) {
                    // satır parse hatası yoksay
                }
            }
        }
    } catch (err) {
        console.warn("Bulut geçmişi okunamadı:", err);
    }

    // 2. Gerçek zamanlı SSE (Server-Sent Events) akışına abone ol (diğer ortak veri girdiğinde anında yakalar)
    if ("EventSource" in window) {
        try {
            cloudEventSource = new EventSource(`https://ntfy.sh/${topic}/sse`);
            cloudEventSource.onmessage = (event) => {
                try {
                    const outer = JSON.parse(event.data);
                    if (outer && outer.message) {
                        const payload = JSON.parse(outer.message);
                        if (payload && payload.senderId !== sessionClientId && payload.data) {
                            applyIncomingData(payload.data, payload.data.updatedBy || "Ortak Cihaz");
                        }
                    }
                } catch (e) {
                    // ignore
                }
            };
        } catch (err) {
            console.warn("Canlı bulut akışı başlatılamadı:", err);
        }
    }
}

async function pushToInstantCloud(roomCode, data) {
    const topic = getCloudTopicName(roomCode);
    try {
        await fetch(`https://ntfy.sh/${topic}`, {
            method: "POST",
            body: JSON.stringify({
                senderId: sessionClientId,
                data: data
            })
        });
    } catch (err) {
        console.warn("Anlık bulut gönderimi uyarısı:", err);
    }
}

function applyIncomingData(incomingData, sourceLabel = "Ortak") {
    if (!incomingData || !incomingData.updatedAt) return;
    if (incomingData.updatedAt <= storeData.updatedAt) return;

    isApplyingRemoteUpdate = true;
    storeData = incomingData;
    localStorage.setItem(getStorageKey(currentRoom), JSON.stringify(storeData));

    // Eğer aktif bir input odaklanmışsa kullanıcının yazmasını bozmamak için kontrol et
    const activeEl = document.activeElement;
    const isTypingInTable = activeEl && (activeEl.classList.contains("inline-num-input") || activeEl.classList.contains("sim-qty-input"));
    if (!isTypingInTable) {
        renderAll();
    } else {
        renderKPIs();
    }

    const lastTimeEl = document.getElementById("last-sync-time");
    if (lastTimeEl) {
        lastTimeEl.textContent = `${storeData.updatedBy || sourceLabel} güncelledi`;
    }
    showToast(`🔄 ${storeData.updatedBy || "Ortağınız"} verileri güncelledi`);
    isApplyingRemoteUpdate = false;
}

async function saveAndBroadcast(actionDescription) {
    if (isApplyingRemoteUpdate) return;

    storeData.updatedAt = Date.now();
    storeData.updatedBy = currentPartner || "Ortak";

    if (actionDescription) {
        storeData.logs = storeData.logs || [];
        storeData.logs.unshift({
            id: "log_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6),
            author: currentPartner || "Ortak",
            action: actionDescription,
            time: Date.now()
        });
        if (storeData.logs.length > 40) {
            storeData.logs = storeData.logs.slice(0, 40);
        }
    }

    // 1. LocalStorage'a kaydet
    localStorage.setItem(getStorageKey(currentRoom), JSON.stringify(storeData));

    // 2. BroadcastChannel ile diğer açık sekmelere anında ilet
    if (broadcastChannel) {
        broadcastChannel.postMessage({
            type: "SYNC_STATE",
            payload: storeData
        });
    }

    // 3. Firebase Bulut veritabanı bağlıysa PUT ile gönder
    if (firebaseRestUrl) {
        pushToFirebaseCloud(currentRoom, storeData);
    }

    // 4. Varsayılan Anlık Bulut Senkronizasyonuna Gönder (Farklı telefon/bilgisayarlar için)
    pushToInstantCloud(currentRoom, storeData);

    const lastTimeEl = document.getElementById("last-sync-time");
    if (lastTimeEl) {
        lastTimeEl.textContent = "Az önce senkronize edildi";
    }

    renderAll();
}

// FIREBASE REALTIME DATABASE REST & STREAM ENTEGRASYONU
function normalizeFirebaseUrl(rawinput) {
    if (!rawinput) return "";
    let trimmed = rawinput.trim();
    // Eğer kullanıcı tam JSON yapıştırdıysa içinden databaseURL'yi ayıkla
    if (trimmed.startsWith("{")) {
        try {
            const parsed = JSON.parse(trimmed);
            if (parsed.databaseURL) {
                trimmed = parsed.databaseURL;
            }
        } catch (e) {
            // JSON değilse devam et
        }
    }
    trimmed = trimmed.replace(/\/+$/, "");
    return trimmed;
}

async function pushToFirebaseCloud(roomCode, data) {
    const baseUrl = normalizeFirebaseUrl(firebaseRestUrl);
    if (!baseUrl) return;
    try {
        const endpoint = `${baseUrl}/kuruyemis_odalar/${encodeURIComponent(roomCode)}.json`;
        await fetch(endpoint, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(data)
        });
    } catch (err) {
        console.warn("Firebase bulut yazma uyarısı:", err);
    }
}

async function pullFromFirebaseCloud(roomCode) {
    const baseUrl = normalizeFirebaseUrl(firebaseRestUrl);
    if (!baseUrl) return;
    try {
        const endpoint = `${baseUrl}/kuruyemis_odalar/${encodeURIComponent(roomCode)}.json`;
        const res = await fetch(endpoint);
        if (!res.ok) return;
        const remoteData = await res.json();
        if (remoteData && remoteData.updatedAt) {
            if (remoteData.updatedAt > storeData.updatedAt) {
                applyIncomingData(remoteData, "Firebase Bulut");
            } else if (storeData.updatedAt > remoteData.updatedAt) {
                await pushToFirebaseCloud(roomCode, storeData);
            }
        } else if (remoteData === null) {
            // Bulutta henüz bu oda yoksa ilk veriyi yükle
            await pushToFirebaseCloud(roomCode, storeData);
        }
    } catch (err) {
        console.warn("Firebase bulut okuma uyarısı:", err);
    }
}

function setupFirebaseCloudSync(roomCode) {
    if (firebasePollingInterval) {
        clearInterval(firebasePollingInterval);
        firebasePollingInterval = null;
    }
    const baseUrl = normalizeFirebaseUrl(firebaseRestUrl);
    if (!baseUrl) return;

    // İlk çekmeyi hemen yap, ardından her 3 saniyede bir kontrol et
    pullFromFirebaseCloud(roomCode);
    firebasePollingInterval = setInterval(() => {
        pullFromFirebaseCloud(roomCode);
    }, 3000);
}

function updateSyncBadgeUI() {
    const pill = document.getElementById("sync-status-pill");
    const text = document.getElementById("sync-status-text");
    const baseUrl = normalizeFirebaseUrl(firebaseRestUrl);

    if (baseUrl) {
        pill.classList.add("cloud");
        text.textContent = "Özel Firebase Bulut Aktif";
    } else {
        pill.classList.remove("cloud");
        text.textContent = "Canlı Bulut Senkronizasyonu Aktif";
    }
}

// ============================================================================
// ARAYÜZ RENDER FONKSİYONLARI
// ============================================================================

function renderAll() {
    renderKPIs();
    renderProductsTable();
    renderExpensesAndBreakeven();
    renderSimulationTable();
    renderDailyTracker();
    renderNotesAndLogs();
    if (window.lucide) {
        window.lucide.createIcons();
    }
}

// 1. ÜST ÖZET KARTLARI (KPI)
function renderKPIs() {
    const products = storeData.products || [];
    const expenses = storeData.expenses || [];

    const totalMonthlyExpenses = expenses.reduce((acc, e) => acc + (Number(e.amount) || 0), 0);
    const dailyExpenseBurden = totalMonthlyExpenses / 30;

    let totalMarginSum = 0;
    let totalUnitProfitSum = 0;
    let totalSimDailyRev = 0;
    let totalSimDailyGrossProfit = 0;
    let totalSimDailyKg = 0;

    products.forEach((p) => {
        const m = calculateProductMetrics(p);
        totalMarginSum += m.grossMargin;
        totalUnitProfitSum += m.unitProfit;
        totalSimDailyRev += m.dailyRevenue;
        totalSimDailyGrossProfit += m.dailyGrossProfit;
        totalSimDailyKg += Number(p.dailySimQty) || 0;
    });

    const avgMargin = products.length > 0 ? totalMarginSum / products.length : 0;
    const avgUnitProfit = products.length > 0 ? totalUnitProfitSum / products.length : 0;
    const dailyNetProfit = totalSimDailyGrossProfit - dailyExpenseBurden;
    const monthlyNetProfit = dailyNetProfit * 30;
    const perPartnerMonthly = monthlyNetProfit / 2;

    // Kart 1: Ürün & Marj
    document.getElementById("kpi-total-products").textContent = `${products.length} Çeşit`;
    document.getElementById("kpi-avg-margin").textContent = `%${avgMargin.toFixed(1)} Ort. Marj`;
    document.getElementById("kpi-market-position").textContent =
        `Ortalama birim net kâr: ${formatTL(avgUnitProfit)}`;

    // Kart 2: Sabit Giderler
    document.getElementById("kpi-monthly-expenses").textContent = formatTLShort(totalMonthlyExpenses);
    document.getElementById("kpi-daily-expense-burden").textContent = `Günlük: ${formatTLShort(dailyExpenseBurden)}`;

    // Kart 3: Simülasyon Ciro
    document.getElementById("kpi-sim-revenue").textContent = formatTLShort(totalSimDailyRev);
    document.getElementById("kpi-sim-gross-profit").textContent = `Brüt: ${formatTLShort(totalSimDailyGrossProfit)}`;
    document.getElementById("kpi-sim-total-kg").textContent =
        `Günlük toplam ${totalSimDailyKg.toFixed(1)} kg/adet satış`;

    // Kart 4: Aylık Net Kâr & Ortak Payı
    const monthlyNetEl = document.getElementById("kpi-monthly-net-profit");
    monthlyNetEl.textContent = formatTLShort(monthlyNetProfit);
    monthlyNetEl.className = monthlyNetProfit >= 0 ? "text-success" : "text-danger";

    document.getElementById("kpi-per-partner-profit").textContent =
        `Ortak Başı: ${formatTLShort(perPartnerMonthly)}`;

    const beStatusEl = document.getElementById("kpi-breakeven-status");
    if (dailyNetProfit >= 0) {
        beStatusEl.textContent = `✅ Sabit giderler çıktıktan sonra günde +${formatTLShort(dailyNetProfit)} net kâr`;
        beStatusEl.className = "text-success";
    } else {
        beStatusEl.textContent = `⚠️ Başabaş için günde ${formatTLShort(Math.abs(dailyNetProfit))} ek brüt kâr gerekli`;
        beStatusEl.className = "text-danger";
    }
}

// 2. ÜRÜN YÖNETİMİ VE KÂR ANALİZİ TABLOSU
function renderProductsTable() {
    const tbody = document.getElementById("products-tbody");
    const searchVal = (document.getElementById("product-search")?.value || "").toLowerCase().trim();
    const catFilter = document.getElementById("category-filter")?.value || "ALL";

    const filtered = (storeData.products || []).filter((p) => {
        const matchesSearch =
            p.name.toLowerCase().includes(searchVal) ||
            p.category.toLowerCase().includes(searchVal);
        const matchesCat = catFilter === "ALL" || p.category === catFilter;
        return matchesSearch && matchesCat;
    });

    if (filtered.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="center-col" style="padding: 32px; color: var(--text-muted);">
                    Arama kriterlerine uygun kuruyemiş/ürün bulunamadı.
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = filtered
        .map((p) => {
            const m = calculateProductMetrics(p);
            const marginClass =
                m.grossMargin >= 30 ? "high" : m.grossMargin >= 18 ? "medium" : "low";

            return `
                <tr data-id="${p.id}">
                    <td>
                        <div class="prod-title-cell">
                            <strong>${escapeHtml(p.name)}</strong>
                            <div class="prod-meta-row">
                                <span class="cat-badge">${escapeHtml(p.category)}</span>
                                <span class="waste-tag">Birim: ${escapeHtml(p.unit)} • Fire: %${Number(p.wastePercent).toFixed(1)}</span>
                            </div>
                        </div>
                    </td>
                    <td class="num-col">
                        <input type="number" step="1" class="inline-num-input quick-edit-field"
                            data-id="${p.id}" data-field="costPrice" value="${p.costPrice}" title="Alış fiyatını hızlı değiştir">
                        <small class="field-hint">₺/${p.unit}</small>
                    </td>
                    <td class="num-col">
                        <strong>${formatTL(m.realCost)}</strong>
                        <small class="field-hint">Fire dahil</small>
                    </td>
                    <td class="num-col">
                        <input type="number" step="1" class="inline-num-input quick-edit-field"
                            data-id="${p.id}" data-field="targetPrice" value="${p.targetPrice}" title="Hedef satış fiyatını hızlı değiştir">
                        <small class="field-hint">₺/${p.unit}</small>
                    </td>
                    <td class="num-col">
                        <strong class="${m.unitProfit >= 0 ? "text-success" : "text-danger"}">
                            ${m.unitProfit >= 0 ? "+" : ""}${formatTL(m.unitProfit)}
                        </strong>
                        <small class="field-hint">Maliyete +%${m.markupPercent.toFixed(0)}</small>
                    </td>
                    <td class="num-col">
                        <span class="badge-margin ${marginClass}">%${m.grossMargin.toFixed(1)}</span>
                    </td>
                    <td class="action-col">
                        <button type="button" class="icon-btn edit-prod-btn" data-id="${p.id}" title="Detaylı Düzenle">
                            <i data-lucide="pencil"></i>
                        </button>
                        <button type="button" class="icon-btn danger delete-prod-btn" data-id="${p.id}" title="Ürünü Sil">
                            <i data-lucide="trash-2"></i>
                        </button>
                    </td>
                </tr>
            `;
        })
        .join("");
}

// 3. SABİT GİDERLER VE BAŞABAŞ (BREAK-EVEN) ANALİZİ
function renderExpensesAndBreakeven() {
    const expenses = storeData.expenses || [];
    const products = storeData.products || [];

    const totalMonthlyExp = expenses.reduce((sum, item) => sum + (Number(item.amount) || 0), 0);
    const dailyExp = totalMonthlyExp / 30;
    const partnerExp = totalMonthlyExp / 2;

    document.getElementById("total-expense-badge").textContent = `${formatTLShort(totalMonthlyExp)} / Ay`;
    document.getElementById("summary-monthly-exp").textContent = formatTL(totalMonthlyExp);
    document.getElementById("summary-daily-exp").textContent = `${formatTL(dailyExp)} / gün`;
    document.getElementById("summary-partner-exp").textContent = `${formatTL(partnerExp)} / ortak`;
    document.getElementById("be-target-expense").textContent = formatTLShort(totalMonthlyExp);

    // Gider Listesini Render Et
    const expListEl = document.getElementById("expenses-list");
    if (expenses.length === 0) {
        expListEl.innerHTML = `<p style="color: var(--text-muted); font-size:0.85rem;">Henüz sabit gider eklenmemiş.</p>`;
    } else {
        expListEl.innerHTML = expenses
            .map(
                (e) => `
            <div class="expense-item">
                <span class="expense-item-name">${escapeHtml(e.name)}</span>
                <div class="expense-item-right">
                    <strong>${formatTL(e.amount)}</strong>
                    <button type="button" class="icon-btn danger delete-exp-btn" data-id="${e.id}" title="Gideri Sil">
                        <i data-lucide="trash-2"></i>
                    </button>
                </div>
            </div>
        `
            )
            .join("");
    }

    // Karma Sepet (Ağırlıklı Ortalama) Başabaş Hesabı
    let totalSimQty = 0;
    let totalSimGrossProfit = 0;
    let totalSimRevenue = 0;

    products.forEach((p) => {
        const m = calculateProductMetrics(p);
        const qty = Number(p.dailySimQty) || 1;
        totalSimQty += qty;
        totalSimGrossProfit += m.unitProfit * qty;
        totalSimRevenue += Number(p.targetPrice) * qty;
    });

    const weightedAvgUnitProfit = totalSimQty > 0 ? totalSimGrossProfit / totalSimQty : 0;
    const weightedAvgMargin = totalSimRevenue > 0 ? (totalSimGrossProfit / totalSimRevenue) * 100 : 0;

    const mixedDailyKgNeeded = weightedAvgUnitProfit > 0 ? dailyExp / weightedAvgUnitProfit : 0;
    const mixedMonthlyKgNeeded = mixedDailyKgNeeded * 30;
    const mixedDailyRevNeeded = weightedAvgMargin > 0 ? dailyExp / (weightedAvgMargin / 100) : 0;

    document.getElementById("be-avg-unit-profit").textContent = `${formatTL(weightedAvgUnitProfit)}`;
    document.getElementById("be-avg-margin").textContent = `%${weightedAvgMargin.toFixed(1)}`;
    document.getElementById("be-mixed-daily-kg").textContent = `${mixedDailyKgNeeded.toFixed(1)} kg/adet`;
    document.getElementById("be-mixed-monthly-kg").textContent = `${mixedMonthlyKgNeeded.toFixed(0)} kg/adet`;
    document.getElementById("be-mixed-daily-rev").textContent = formatTLShort(mixedDailyRevNeeded);

    // Ürün Bazlı Başabaş Tablosu
    const beTbody = document.getElementById("breakeven-tbody");
    beTbody.innerHTML = products
        .map((p) => {
            const m = calculateProductMetrics(p);
            // Sadece bu üründen satılsa günlük ve aylık kaç kg satılmalı?
            const singleDailyKg = m.unitProfit > 0 ? dailyExp / m.unitProfit : 0;
            const singleMonthlyKg = m.unitProfit > 0 ? totalMonthlyExp / m.unitProfit : 0;

            // Karma sepetteki mevcut satış ağırlığına göre bu ürüne düşen günlük başabaş miktarı
            const weightShare = totalSimQty > 0 ? (Number(p.dailySimQty) || 0) / totalSimQty : 1 / products.length;
            const mixedProductDailyKg = mixedDailyKgNeeded * weightShare;

            return `
                <tr>
                    <td>
                        <strong>${escapeHtml(p.name)}</strong>
                        <small class="field-hint">Satış: ${formatTL(p.targetPrice)} / ${p.unit}</small>
                    </td>
                    <td class="num-col">
                        <strong class="text-success">+${formatTL(m.unitProfit)}</strong>
                    </td>
                    <td class="num-col">
                        <strong>${singleDailyKg.toFixed(1)} ${p.unit} / gün</strong>
                        <small class="field-hint">Tek başına tüm gider için</small>
                    </td>
                    <td class="num-col">
                        <strong>${singleMonthlyKg.toFixed(0)} ${p.unit} / ay</strong>
                    </td>
                    <td class="num-col">
                        <span class="badge-margin high">${mixedProductDailyKg.toFixed(1)} ${p.unit} / gün</span>
                        <small class="field-hint">Karma sepetteki payı</small>
                    </td>
                </tr>
            `;
        })
        .join("");
}

// 4. GÜNLÜK SATIŞ VE ORTAK KÂR SİMÜLASYONU
function renderSimulationTable() {
    const products = storeData.products || [];
    const expenses = storeData.expenses || [];

    const totalMonthlyExp = expenses.reduce((sum, item) => sum + (Number(item.amount) || 0), 0);
    const dailyExp = totalMonthlyExp / 30;

    let totalDailyRev = 0;
    let totalDailyGross = 0;

    const simTbody = document.getElementById("simulation-tbody");
    simTbody.innerHTML = products
        .map((p) => {
            const m = calculateProductMetrics(p);
            totalDailyRev += m.dailyRevenue;
            totalDailyGross += m.dailyGrossProfit;

            return `
                <tr>
                    <td>
                        <strong>${escapeHtml(p.name)}</strong>
                        <small class="field-hint">${escapeHtml(p.category)} (${p.unit})</small>
                    </td>
                    <td class="num-col">${formatTL(p.targetPrice)}</td>
                    <td class="num-col text-success">+${formatTL(m.unitProfit)}</td>
                    <td class="center-col">
                        <div class="sim-qty-control">
                            <button type="button" class="qty-step-btn" data-id="${p.id}" data-step="-1">-</button>
                            <input type="number" step="0.5" min="0" class="sim-qty-input"
                                data-id="${p.id}" value="${p.dailySimQty}">
                            <button type="button" class="qty-step-btn" data-id="${p.id}" data-step="1">+</button>
                        </div>
                    </td>
                    <td class="num-col">
                        <strong>${formatTL(m.dailyRevenue)}</strong>
                    </td>
                    <td class="num-col">
                        <strong class="text-success">+${formatTL(m.dailyGrossProfit)}</strong>
                    </td>
                    <td class="num-col">
                        <strong>${formatTLShort(m.monthlyGrossProfit)}</strong>
                    </td>
                </tr>
            `;
        })
        .join("");

    // Başabaş Karşılama Oranı
    const coveragePercent = dailyExp > 0 ? (totalDailyGross / dailyExp) * 100 : 100;
    const clampedBarWidth = Math.min(100, Math.max(0, coveragePercent));

    document.getElementById("sim-progress-bar").style.width = `${clampedBarWidth}%`;
    document.getElementById("sim-daily-gross-vs-exp").textContent =
        `Günlük Brüt Kâr: ${formatTLShort(totalDailyGross)} / Günlük Sabit Gider: ${formatTLShort(dailyExp)}`;

    const descEl = document.getElementById("sim-progress-desc");
    if (coveragePercent >= 100) {
        descEl.innerHTML = `🎉 <strong>Tebrikler!</strong> Günlük sabit giderlerin <strong>%${coveragePercent.toFixed(0)}</strong>'ini karşılıyorsunuz. Başabaş noktası aşıldı ve net kâra geçildi.`;
    } else {
        const missing = dailyExp - totalDailyGross;
        descEl.innerHTML = `⚠️ Günlük sabit giderlerin <strong>%${coveragePercent.toFixed(0)}</strong>'i karşılanıyor. Başabaş noktasına ulaşmak için günde <strong>${formatTL(missing)}</strong> daha brüt kâr gerekiyor.`;
    }

    const dailyNet = totalDailyGross - dailyExp;
    const monthlyNet = dailyNet * 30;
    const partnerShare = monthlyNet / 2;

    const dailyNetEl = document.getElementById("sim-daily-net-profit");
    dailyNetEl.textContent = (dailyNet >= 0 ? "+" : "") + formatTL(dailyNet);
    dailyNetEl.className = dailyNet >= 0 ? "text-success" : "text-danger";

    const monthlyNetEl = document.getElementById("sim-monthly-net-profit");
    monthlyNetEl.textContent = (monthlyNet >= 0 ? "+" : "") + formatTLShort(monthlyNet);
    monthlyNetEl.className = monthlyNet >= 0 ? "text-success" : "text-danger";

    const partnerShareEl = document.getElementById("sim-partner-share");
    partnerShareEl.textContent = (partnerShare >= 0 ? "+" : "") + formatTLShort(partnerShare) + " / Kişi";
    partnerShareEl.className = partnerShare >= 0 ? "text-success" : "text-danger";
}

// 4.5 CANLI GÜNLÜK SATIŞ TAKİBİ (PARÇA PARÇA KG / GRAMAJ İŞLEME VE HEDEF KONTROLÜ)
function renderDailyTracker() {
    const products = storeData.products || [];
    const expenses = storeData.expenses || [];
    storeData.dailySales = storeData.dailySales || {};

    const dateInput = document.getElementById("tracker-date-input");
    if (dateInput && dateInput.value !== activeTrackerDate) {
        dateInput.value = activeTrackerDate;
    }

    // Ürün seçimi açılır listesini güncelle (seçili değeri koruyarak)
    const selectEl = document.getElementById("sale-product-select");
    if (selectEl) {
        const prevSelected = selectEl.value;
        selectEl.innerHTML = products
            .map((p) => `<option value="${p.id}">${escapeHtml(p.name)} (${formatTL(p.targetPrice)}/${p.unit})</option>`)
            .join("");
        if (prevSelected && products.some((p) => p.id === prevSelected)) {
            selectEl.value = prevSelected;
        }
    }

    const entries = storeData.dailySales[activeTrackerDate] || [];

    // Her ürün için bugün satılan toplam miktarı topla
    const soldByProduct = {};
    entries.forEach((entry) => {
        soldByProduct[entry.productId] = (soldByProduct[entry.productId] || 0) + (Number(entry.qty) || 0);
    });

    const totalMonthlyExp = expenses.reduce((sum, item) => sum + (Number(item.amount) || 0), 0);
    const dailyExp = totalMonthlyExp / 30;

    let totalActualKg = 0;
    let totalTargetKg = 0;
    let totalActualRev = 0;
    let totalTargetRev = 0;
    let totalActualGross = 0;
    let totalTargetGross = 0;

    const tbody = document.getElementById("tracker-products-tbody");
    if (tbody) {
        tbody.innerHTML = products
            .map((p) => {
                const m = calculateProductMetrics(p);
                const soldQty = Number((soldByProduct[p.id] || 0).toFixed(3));
                const targetQty = Number(p.dailySimQty) || 0;

                const actualRev = soldQty * Number(p.targetPrice);
                const actualProfit = soldQty * m.unitProfit;

                totalActualKg += soldQty;
                totalTargetKg += targetQty;
                totalActualRev += actualRev;
                totalTargetRev += m.dailyRevenue;
                totalActualGross += actualProfit;
                totalTargetGross += m.dailyGrossProfit;

                const goalPct = targetQty > 0 ? Math.min(999, (soldQty / targetQty) * 100) : (soldQty > 0 ? 100 : 0);
                const barWidth = Math.min(100, Math.max(0, goalPct));
                const isGoalReached = soldQty >= targetQty && targetQty > 0;

                let statusHtml = "";
                if (isGoalReached) {
                    statusHtml = `<span class="badge-margin high">✅ Hedef Tamam (%${goalPct.toFixed(0)})</span>`;
                } else {
                    const remaining = Math.max(0, targetQty - soldQty);
                    statusHtml = `
                        <div>
                            <div class="goal-mini-bar"><div class="goal-mini-fill" style="width:${barWidth}%"></div></div>
                            <strong>%${goalPct.toFixed(0)}</strong>
                        </div>
                        <small class="field-hint">Hedefe ${remaining.toFixed(2)} ${p.unit} kaldı</small>
                    `;
                }

                return `
                    <tr>
                        <td>
                            <strong>${escapeHtml(p.name)}</strong>
                            <small class="field-hint">${formatTL(p.targetPrice)} / ${p.unit}</small>
                        </td>
                        <td class="center-col">
                            <div class="quick-row-btns">
                                <button type="button" class="quick-add-chip" data-id="${p.id}" data-qty="0.25" title="250 Gram Satış Ekle">+250g</button>
                                <button type="button" class="quick-add-chip" data-id="${p.id}" data-qty="0.5" title="500 Gram Satış Ekle">+500g</button>
                                <button type="button" class="quick-add-chip" data-id="${p.id}" data-qty="1" title="1 Kg Satış Ekle">+1kg</button>
                                <button type="button" class="quick-add-chip" data-id="${p.id}" data-qty="3" title="3 Kg Satış Ekle">+3kg</button>
                            </div>
                        </td>
                        <td class="num-col">
                            <strong class="${soldQty > 0 ? "text-success" : ""}">${soldQty.toLocaleString("tr-TR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ${p.unit}</strong>
                        </td>
                        <td class="num-col">
                            <span>${targetQty.toFixed(1)} ${p.unit}</span>
                        </td>
                        <td class="center-col">
                            ${statusHtml}
                        </td>
                        <td class="num-col">
                            <strong>${formatTL(actualRev)}</strong>
                            <small class="field-hint text-success">Kâr: +${formatTL(actualProfit)}</small>
                        </td>
                    </tr>
                `;
            })
            .join("");
    }

    // Üst İlerleme Kartı ve Özet Kutuları
    const overallGoalPct = totalTargetGross > 0 ? (totalActualGross / totalTargetGross) * 100 : 0;
    const clampedOverall = Math.min(100, Math.max(0, overallGoalPct));

    document.getElementById("tracker-progress-bar").style.width = `${clampedOverall}%`;
    document.getElementById("tracker-profit-vs-goal").textContent =
        `Gerçekleşen Brüt Kâr: ${formatTLShort(totalActualGross)} / Günlük Hedef Kâr: ${formatTLShort(totalTargetGross)}`;

    const goalDescEl = document.getElementById("tracker-goal-desc");
    if (overallGoalPct >= 100 && totalTargetGross > 0) {
        goalDescEl.innerHTML = `🏆 <strong>Harika!</strong> Bugünkü satışlarınızla günlük kâr hedefinizin <strong>%${overallGoalPct.toFixed(0)}</strong> seviyesine ulaştınız ve hedefi geçtiniz!`;
    } else {
        const remProfit = Math.max(0, totalTargetGross - totalActualGross);
        goalDescEl.innerHTML = `📊 Bugünkü satışlarınız günlük hedefinizin <strong>%${overallGoalPct.toFixed(1)}</strong>'ine ulaştı. Günlük hedefi yakalamak için <strong>${formatTL(remProfit)}</strong> daha brüt kâr gerekiyor.`;
    }

    document.getElementById("tracker-total-kg").textContent = `${totalActualKg.toLocaleString("tr-TR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} kg / adet`;
    document.getElementById("tracker-target-kg-sub").textContent = `Günlük Hedef: ${totalTargetKg.toFixed(1)} kg / adet`;

    document.getElementById("tracker-total-rev").textContent = formatTL(totalActualRev);
    document.getElementById("tracker-target-rev-sub").textContent = `Günlük Hedef Ciro: ${formatTLShort(totalTargetRev)}`;

    document.getElementById("tracker-total-gross").textContent = "+" + formatTL(totalActualGross);
    document.getElementById("tracker-daily-exp-sub").textContent = `Günlük Sabit Gider Yükü: ${formatTLShort(dailyExp)}`;

    const netDayProfit = totalActualGross - dailyExp;
    const netEl = document.getElementById("tracker-net-profit");
    netEl.textContent = (netDayProfit >= 0 ? "+" : "") + formatTL(netDayProfit);
    netEl.className = netDayProfit >= 0 ? "text-success" : "text-danger";

    const beBadgeEl = document.getElementById("tracker-breakeven-badge");
    if (netDayProfit >= 0) {
        beBadgeEl.textContent = `✅ Bugünün sabit gideri çıktı, net kârdasınız!`;
        beBadgeEl.className = "field-hint text-success";
    } else {
        beBadgeEl.textContent = `⏳ Başabaş için ${formatTLShort(Math.abs(netDayProfit))} kâr kaldı`;
        beBadgeEl.className = "field-hint text-danger";
    }

    // Sağ Taraf: Parça Parça Girilen Satış Hareketleri Listesi
    document.getElementById("tracker-entries-count").textContent = `${entries.length} Satış Kaydı`;
    const entriesListEl = document.getElementById("tracker-entries-list");
    if (entries.length === 0) {
        entriesListEl.innerHTML = `<p style="color: var(--text-muted); font-size:0.85rem;">Bu tarihte henüz satış girişi yapılmadı. Yukarıdan veya tablodaki hızlı butonlardan satış ekleyebilirsiniz.</p>`;
    } else {
        entriesListEl.innerHTML = entries
            .map((item) => {
                const prod = products.find((p) => p.id === item.productId);
                const prodName = prod ? prod.name : item.productName || "Ürün";
                const unit = prod ? prod.unit : "kg";
                const m = prod ? calculateProductMetrics(prod) : { unitProfit: 0 };
                const itemRev = prod ? Number(prod.targetPrice) * Number(item.qty) : 0;
                const itemProfit = m.unitProfit * Number(item.qty);
                const timeStr = item.time ? new Date(item.time).toLocaleTimeString("tr-TR", { hour: "2-digit", minute: "2-digit" }) : "";

                return `
                    <div class="note-card">
                        <div class="note-meta">
                            <strong>🕒 ${timeStr} • 👤 ${escapeHtml(item.author || "Ortak")}</strong>
                            <div class="note-meta-right">
                                <button type="button" class="note-action-btn danger delete-sale-entry-btn" data-id="${item.id}" title="Bu Satış Kaydını Sil">
                                    <i data-lucide="trash-2"></i>
                                    <span>Kaldır</span>
                                </button>
                            </div>
                        </div>
                        <p class="note-text"><strong>+${Number(item.qty).toLocaleString("tr-TR")} ${unit}</strong> ${escapeHtml(prodName)} satıldı.</p>
                        <small class="field-hint">Ciro: ${formatTL(itemRev)} • Net Kâr: <strong class="text-success">+${formatTL(itemProfit)}</strong> ${item.note ? `• Not: ${escapeHtml(item.note)}` : ""}</small>
                    </div>
                `;
            })
            .join("");
    }
}

function addDailySaleEntry(productId, qtyVal, noteText = "") {
    const qty = Number(qtyVal);
    if (!productId || isNaN(qty) || qty <= 0) return;

    const prod = (storeData.products || []).find((p) => p.id === productId);
    if (!prod) return;

    storeData.dailySales = storeData.dailySales || {};
    storeData.dailySales[activeTrackerDate] = storeData.dailySales[activeTrackerDate] || [];

    storeData.dailySales[activeTrackerDate].unshift({
        id: "sale_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6),
        productId: prod.id,
        productName: prod.name,
        qty: Number(qty.toFixed(3)),
        note: noteText.trim(),
        author: currentPartner || "Ortak",
        time: Date.now()
    });

    const totalSoldToday = storeData.dailySales[activeTrackerDate]
        .filter((x) => x.productId === prod.id)
        .reduce((s, x) => s + Number(x.qty), 0);

    saveAndBroadcast(`Günlük satışa +${qty} ${prod.unit} ${prod.name} işledi (Bugün toplam: ${totalSoldToday.toFixed(2)} ${prod.unit})`);
    showToast(`⚖️ +${qty} ${prod.unit} ${prod.name} eklendi! (Bugün toplam: ${totalSoldToday.toFixed(2)} ${prod.unit})`);
}

// 5. ORTAK NOTLAR VE İŞLEM GEÇMİŞİ
function renderNotesAndLogs() {
    if (Array.isArray(storeData.notes)) {
        storeData.notes = storeData.notes.filter((n) => n.id !== "n1" && n.id !== "n2");
    }
    const notes = storeData.notes || [];
    const logs = storeData.logs || [];

    document.getElementById("notes-count-badge").textContent = notes.length;

    const notesEl = document.getElementById("notes-list");
    if (notes.length === 0) {
        notesEl.innerHTML = `<p style="color: var(--text-muted); font-size:0.85rem;">Henüz ortak notu eklenmemiş. Yukarıdaki kutucuktan yeni not ekleyebilirsiniz.</p>`;
    } else {
        notesEl.innerHTML = notes
            .map(
                (n) => `
            <div class="note-card" data-id="${n.id}">
                <div class="note-meta">
                    <strong>👤 ${escapeHtml(n.author)}</strong>
                    <div class="note-meta-right">
                        <span>${formatTimeAgo(n.createdAt)}</span>
                        <button type="button" class="note-action-btn edit-note-btn" data-id="${n.id}" title="Notu Düzenle">
                            <i data-lucide="pencil"></i>
                            <span>Notu Düzenle</span>
                        </button>
                        <button type="button" class="note-action-btn danger delete-note-btn" data-id="${n.id}" title="Notu Kaldır">
                            <i data-lucide="trash-2"></i>
                            <span>Notu Kaldır</span>
                        </button>
                    </div>
                </div>
                <p class="note-text">${escapeHtml(n.text)}</p>
            </div>
        `
            )
            .join("");
    }

    const logsEl = document.getElementById("activity-log-list");
    logsEl.innerHTML = logs
        .map(
            (l) => `
        <div class="log-item">
            <span><strong>${escapeHtml(l.author)}:</strong> ${escapeHtml(l.action)}</span>
            <span class="log-time">${formatTimeAgo(l.time)}</span>
        </div>
    `
        )
        .join("");
}

// ============================================================================
// ETKİLEŞİM VE EVENT LISTENER'LAR
// ============================================================================

function escapeHtml(str) {
    return String(str || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function showToast(message) {
    const container = document.getElementById("toast-container");
    const toast = document.createElement("div");
    toast.className = "toast";
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => {
        toast.remove();
    }, 3200);
}

function setupEventListeners() {
    // URL parametresinden ortak oda kodu geldiyse otomatik doldur
    const urlParams = new URLSearchParams(window.location.search);
    const roomParam = urlParams.get("oda");
    if (roomParam) {
        currentRoom = roomParam.trim().toLowerCase();
        localStorage.setItem("ky_room_code", currentRoom);
    }

    const authModal = document.getElementById("auth-modal");
    const partnerNameInput = document.getElementById("partner-name");
    const roomCodeInput = document.getElementById("room-code");

    partnerNameInput.value = currentPartner || "Enes";
    roomCodeInput.value = currentRoom || "bereket-kuruyemis";

    // Ortak Seçim Kartları (Enes & Ali Osman)
    const partnerCards = document.querySelectorAll(".partner-select-card");
    const syncPartnerCardHighlight = (nameVal) => {
        partnerCards.forEach((card) => {
            if (card.getAttribute("data-partner").toLowerCase() === (nameVal || "").trim().toLowerCase()) {
                card.classList.add("active");
            } else {
                card.classList.remove("active");
            }
        });
    };
    syncPartnerCardHighlight(partnerNameInput.value);

    partnerCards.forEach((card) => {
        card.addEventListener("click", () => {
            const chosenName = card.getAttribute("data-partner");
            partnerNameInput.value = chosenName;
            syncPartnerCardHighlight(chosenName);
        });
    });

    partnerNameInput.addEventListener("input", () => {
        syncPartnerCardHighlight(partnerNameInput.value);
    });

    // Eğer daha önce giriş yapıldıysa doğrudan aç
    if (currentPartner && currentRoom) {
        authModal.classList.add("hidden");
        applySessionHeader();
        initSyncEngine(currentRoom);
    } else {
        initSyncEngine(currentRoom);
    }

    // Giriş Formu
    document.getElementById("login-form").addEventListener("submit", (e) => {
        e.preventDefault();
        const enteredPin = document.getElementById("room-pin").value.trim();
        if (enteredPin !== "1461") {
            showToast("❌ Hatalı ortak giriş şifresi! (Şifre: 1461)");
            return;
        }
        currentPartner = partnerNameInput.value.trim() || "Ortak";
        currentRoom = roomCodeInput.value.trim().toLowerCase() || "bereket-kuruyemis";

        localStorage.setItem("ky_partner_name", currentPartner);
        localStorage.setItem("ky_room_code", currentRoom);

        authModal.classList.add("hidden");
        applySessionHeader();
        initSyncEngine(currentRoom);
        showToast(`🌰 Hoş geldin ${currentPartner}! "${currentRoom}" odasına bağlandınız.`);
    });

    // Odayı Değiştir / Çıkış
    document.getElementById("logout-btn").addEventListener("click", () => {
        authModal.classList.remove("hidden");
    });

    // Ortak Davet Linkini Kopyala
    document.getElementById("share-room-btn").addEventListener("click", () => {
        const shareUrl = `${window.location.origin}${window.location.pathname}?oda=${encodeURIComponent(currentRoom)}`;
        navigator.clipboard?.writeText(shareUrl);
        showToast(`🔗 Ortak davet linki kopyalandı! Ortağınıza gönderebilirsiniz.`);
    });

    // Sekme (Tab) Geçişleri
    document.querySelectorAll(".tab-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
            document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
            btn.classList.add("active");
            const targetId = btn.getAttribute("data-tab");
            document.getElementById(targetId)?.classList.add("active");
        });
    });

    // Ürün Arama ve Kategori Filtresi
    document.getElementById("product-search").addEventListener("input", renderProductsTable);
    document.getElementById("category-filter").addEventListener("change", renderProductsTable);

    // Tablo İçinden Hızlı Fiyat Değiştirme + Düzenleme / Silme
    document.getElementById("products-tbody").addEventListener("change", (e) => {
        const target = e.target;
        if (target.classList.contains("quick-edit-field")) {
            const prodId = target.getAttribute("data-id");
            const field = target.getAttribute("data-field");
            const newVal = Number(target.value);
            const prod = storeData.products.find((p) => p.id === prodId);
            if (prod && newVal >= 0) {
                prod[field] = newVal;
                const fieldLabels = {
                    costPrice: "alış fiyatını",
                    targetPrice: "satış fiyatını",
                    marketPrice: "piyasa fiyatını"
                };
                saveAndBroadcast(`${prod.name} ${fieldLabels[field] || "fiyatını"} ${newVal} ₺ olarak güncelledi`);
                showToast(`✅ ${prod.name} güncellendi`);
            }
        }
    });

    document.getElementById("products-tbody").addEventListener("click", (e) => {
        const editBtn = e.target.closest(".edit-prod-btn");
        if (editBtn) {
            openProductModal(editBtn.getAttribute("data-id"));
            return;
        }
        const delBtn = e.target.closest(".delete-prod-btn");
        if (delBtn) {
            const id = delBtn.getAttribute("data-id");
            const prod = storeData.products.find((p) => p.id === id);
            if (prod && confirm(`"${prod.name}" ürününü listeden silmek istediğinize emin misiniz?`)) {
                storeData.products = storeData.products.filter((p) => p.id !== id);
                saveAndBroadcast(`${prod.name} ürününü sildi`);
                showToast(`🗑️ ${prod.name} silindi`);
            }
        }
    });

    // Yeni Ürün Modalı Aç/Kapat ve Canlı Önizleme
    document.getElementById("add-product-btn").addEventListener("click", () => openProductModal(null));
    document.getElementById("close-product-modal").addEventListener("click", closeProductModal);
    document.getElementById("cancel-product-btn").addEventListener("click", closeProductModal);

    ["prod-cost", "prod-price", "prod-waste"].forEach((id) => {
        document.getElementById(id).addEventListener("input", updateModalLivePreview);
    });

    document.getElementById("product-form").addEventListener("submit", (e) => {
        e.preventDefault();
        const id = document.getElementById("product-id").value;
        const name = document.getElementById("prod-name").value.trim();
        const category = document.getElementById("prod-category").value;
        const unit = document.getElementById("prod-unit").value;
        const wastePercent = Number(document.getElementById("prod-waste").value) || 0;
        const costPrice = Number(document.getElementById("prod-cost").value) || 0;
        const targetPrice = Number(document.getElementById("prod-price").value) || 0;

        if (id) {
            const existing = storeData.products.find((p) => p.id === id);
            if (existing) {
                Object.assign(existing, {
                    name,
                    category,
                    unit,
                    wastePercent,
                    costPrice,
                    targetPrice
                });
                saveAndBroadcast(`${name} ürün kartını güncelledi`);
                showToast(`✅ ${name} güncellendi`);
            }
        } else {
            storeData.products.unshift({
                id: "p_" + Date.now(),
                name,
                category,
                unit,
                wastePercent,
                costPrice,
                targetPrice,
                dailySimQty: 5.0
            });
            saveAndBroadcast(`Yeni ürün ekledi: ${name}`);
            showToast(`🌰 ${name} eklendi`);
        }
        closeProductModal();
    });

    // Sabit Gider Ekleme & Silme
    document.getElementById("expense-form").addEventListener("submit", (e) => {
        e.preventDefault();
        const nameEl = document.getElementById("exp-name");
        const amountEl = document.getElementById("exp-amount");
        const name = nameEl.value.trim();
        const amount = Number(amountEl.value) || 0;

        if (name && amount > 0) {
            storeData.expenses.push({
                id: "e_" + Date.now(),
                name,
                amount
            });
            nameEl.value = "";
            amountEl.value = "";
            saveAndBroadcast(`Sabit gider ekledi: ${name} (${formatTLShort(amount)})`);
            showToast(`🏢 ${name} giderlere eklendi`);
        }
    });

    document.getElementById("expenses-list").addEventListener("click", (e) => {
        const delBtn = e.target.closest(".delete-exp-btn");
        if (delBtn) {
            const id = delBtn.getAttribute("data-id");
            const exp = storeData.expenses.find((x) => x.id === id);
            if (exp) {
                storeData.expenses = storeData.expenses.filter((x) => x.id !== id);
                saveAndBroadcast(`${exp.name} sabit giderini kaldırdı`);
                showToast(`🗑️ ${exp.name} silindi`);
            }
        }
    });

    // Satış Simülasyonu Miktar Değişimleri
    document.getElementById("simulation-tbody").addEventListener("click", (e) => {
        const stepBtn = e.target.closest(".qty-step-btn");
        if (stepBtn) {
            const id = stepBtn.getAttribute("data-id");
            const step = Number(stepBtn.getAttribute("data-step")) || 0;
            const prod = storeData.products.find((p) => p.id === id);
            if (prod) {
                prod.dailySimQty = Math.max(0, Number((Number(prod.dailySimQty) + step).toFixed(1)));
                saveAndBroadcast(`${prod.name} günlük satış simülasyonunu ${prod.dailySimQty} ${prod.unit} yaptı`);
            }
        }
    });

    document.getElementById("simulation-tbody").addEventListener("change", (e) => {
        if (e.target.classList.contains("sim-qty-input")) {
            const id = e.target.getAttribute("data-id");
            const val = Math.max(0, Number(e.target.value) || 0);
            const prod = storeData.products.find((p) => p.id === id);
            if (prod) {
                prod.dailySimQty = val;
                saveAndBroadcast(`${prod.name} günlük satış simülasyonunu ${val} ${prod.unit} yaptı`);
            }
        }
    });

    // Simülasyon Hazır Senaryo Çarpanları
    document.querySelectorAll(".sim-preset-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
            const mult = Number(btn.getAttribute("data-multiplier")) || 1;
            storeData.products.forEach((p, idx) => {
                const defaultProd = DEFAULT_STORE_DATA.products[idx] || { dailySimQty: 6 };
                p.dailySimQty = Number((defaultProd.dailySimQty * mult).toFixed(1));
            });
            saveAndBroadcast(`Satış simülasyonunda %${Math.round(mult * 100)} senaryosunu uyguladı`);
            showToast(`📊 Satış simülasyonu %${Math.round(mult * 100)} temposuna göre güncellendi`);
        });
    });

    // Canlı Günlük Satış Takibi Event Listener'ları
    const trackerDateInput = document.getElementById("tracker-date-input");
    if (trackerDateInput) {
        trackerDateInput.value = activeTrackerDate;
        trackerDateInput.addEventListener("change", () => {
            if (trackerDateInput.value) {
                activeTrackerDate = trackerDateInput.value;
                renderDailyTracker();
                if (window.lucide) window.lucide.createIcons();
            }
        });
    }

    document.getElementById("tracker-today-btn")?.addEventListener("click", () => {
        activeTrackerDate = new Date().toISOString().slice(0, 10);
        if (trackerDateInput) trackerDateInput.value = activeTrackerDate;
        renderDailyTracker();
        if (window.lucide) window.lucide.createIcons();
    });

    document.getElementById("tracker-clear-day-btn")?.addEventListener("click", () => {
        storeData.dailySales = storeData.dailySales || {};
        const currentEntries = storeData.dailySales[activeTrackerDate] || [];
        if (currentEntries.length === 0) {
            showToast("Bu tarihte silinecek satış kaydı yok.");
            return;
        }
        if (confirm(`${activeTrackerDate} tarihindeki tüm günlük satış kayıtlarını sıfırlamak istediğinize emin misiniz?`)) {
            storeData.dailySales[activeTrackerDate] = [];
            saveAndBroadcast(`${activeTrackerDate} günü satış kayıtlarını sıfırladı`);
            showToast(`🗑️ ${activeTrackerDate} günü satışları sıfırlandı`);
        }
    });

    // Gramaj Hazır Butonları (250g, 500g, 1Kg, 2Kg, 3Kg, 5Kg)
    document.querySelectorAll(".gram-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
            const val = Number(btn.getAttribute("data-val")) || 1;
            const qtyInput = document.getElementById("sale-qty-input");
            if (qtyInput) qtyInput.value = val;
        });
    });

    // Hızlı Satış Ekleme Formu
    document.getElementById("quick-sale-form")?.addEventListener("submit", (e) => {
        e.preventDefault();
        const prodId = document.getElementById("sale-product-select")?.value;
        const qtyVal = document.getElementById("sale-qty-input")?.value;
        const noteInput = document.getElementById("sale-note-input");
        const noteVal = noteInput ? noteInput.value : "";

        addDailySaleEntry(prodId, qtyVal, noteVal);
        if (noteInput) noteInput.value = "";
    });

    // Tablo Satırından Tek Tıkla (+250g, +500g, +1kg, +3kg) Satış Ekleme
    document.getElementById("tracker-products-tbody")?.addEventListener("click", (e) => {
        const chip = e.target.closest(".quick-add-chip");
        if (chip) {
            const prodId = chip.getAttribute("data-id");
            const qtyVal = Number(chip.getAttribute("data-qty")) || 1;
            addDailySaleEntry(prodId, qtyVal, "Hızlı Buton");
        }
    });

    // Sağdaki Listeden Parça Satış Kaydını Silme
    document.getElementById("tracker-entries-list")?.addEventListener("click", (e) => {
        const delBtn = e.target.closest(".delete-sale-entry-btn");
        if (delBtn) {
            const entryId = delBtn.getAttribute("data-id");
            storeData.dailySales = storeData.dailySales || {};
            const list = storeData.dailySales[activeTrackerDate] || [];
            const target = list.find((x) => x.id === entryId);
            if (target) {
                storeData.dailySales[activeTrackerDate] = list.filter((x) => x.id !== entryId);
                saveAndBroadcast(`Günlük satıştan ${target.qty} birim ${target.productName || "ürün"} kaydını kaldırdı`);
                showToast(`🗑️ Satış kaydı kaldırıldı`);
            }
        }
    });

    // Ortak Not Ekleme / Düzenleme / Kaldırma
    const noteForm = document.getElementById("note-form");
    const noteInput = document.getElementById("note-input");
    const editingNoteIdInput = document.getElementById("editing-note-id");
    const noteSubmitText = document.getElementById("note-submit-text");
    const noteCancelEditBtn = document.getElementById("note-cancel-edit-btn");

    const resetNoteFormState = () => {
        editingNoteIdInput.value = "";
        noteInput.value = "";
        noteSubmitText.textContent = "Yeni Not Ekle";
        noteCancelEditBtn.classList.add("hidden");
    };

    noteCancelEditBtn.addEventListener("click", resetNoteFormState);

    noteForm.addEventListener("submit", (e) => {
        e.preventDefault();
        const text = noteInput.value.trim();
        const editingId = editingNoteIdInput.value;
        if (!text) return;

        storeData.notes = storeData.notes || [];

        if (editingId) {
            const targetNote = storeData.notes.find((n) => n.id === editingId);
            if (targetNote) {
                targetNote.text = text;
                targetNote.author = currentPartner || targetNote.author || "Ortak";
                targetNote.createdAt = Date.now();
                saveAndBroadcast(`Ortak panodaki bir notu düzenledi`);
                showToast(`✏️ Not güncellendi`);
            }
        } else {
            storeData.notes.unshift({
                id: "n_" + Date.now(),
                author: currentPartner || "Ortak",
                text,
                createdAt: Date.now()
            });
            saveAndBroadcast(`Ortak panoya yeni bir not ekledi`);
            showToast(`💬 Yeni not eklendi ve ortağınızla paylaşıldı`);
        }

        resetNoteFormState();
    });

    document.getElementById("notes-list").addEventListener("click", (e) => {
        const editBtn = e.target.closest(".edit-note-btn");
        if (editBtn) {
            const id = editBtn.getAttribute("data-id");
            const note = (storeData.notes || []).find((n) => n.id === id);
            if (note) {
                editingNoteIdInput.value = note.id;
                noteInput.value = note.text;
                noteSubmitText.textContent = "Notu Güncelle";
                noteCancelEditBtn.classList.remove("hidden");
                noteInput.focus();
            }
            return;
        }

        const delBtn = e.target.closest(".delete-note-btn");
        if (delBtn) {
            const id = delBtn.getAttribute("data-id");
            const note = (storeData.notes || []).find((n) => n.id === id);
            if (note && confirm("Bu notu kaldırmak istediğinize emin misiniz?")) {
                storeData.notes = (storeData.notes || []).filter((n) => n.id !== id);
                if (editingNoteIdInput.value === id) {
                    resetNoteFormState();
                }
                saveAndBroadcast(`Ortak panodan bir notu kaldırdı`);
                showToast(`🗑️ Not kaldırıldı`);
            }
        }
    });

    // JSON Yedek İndir / Yükle / Sıfırla
    document.getElementById("export-json-btn").addEventListener("click", () => {
        const blob = new Blob([JSON.stringify(storeData, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `${currentRoom}-yedek-${new Date().toISOString().slice(0, 10)}.json`;
        a.click();
        URL.revokeObjectURL(url);
    });

    document.getElementById("import-json-input").addEventListener("change", (e) => {
        const file = e.target.files?.[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = (ev) => {
            try {
                const imported = JSON.parse(ev.target.result);
                if (imported && Array.isArray(imported.products)) {
                    storeData = imported;
                    saveAndBroadcast(`Yedek dosyasından verileri geri yükledi`);
                    showToast(`✅ Yedek başarıyla yüklendi!`);
                }
            } catch (err) {
                alert("Geçersiz JSON yedek dosyası.");
            }
        };
        reader.readAsText(file);
    });

    document.getElementById("reset-demo-btn").addEventListener("click", () => {
        if (confirm("Tüm verileri varsayılan kuruyemiş dükkanı örnek verilerine sıfırlamak istiyor musunuz?")) {
            storeData = structuredClone(DEFAULT_STORE_DATA);
            saveAndBroadcast(`Verileri fabrika örnek ayarlarına sıfırladı`);
            showToast(`🔄 Örnek kuruyemiş verileri yüklendi`);
        }
    });

    // Firebase Ayarları Modalı
    const fbModal = document.getElementById("firebase-modal");
    const openFbModal = () => {
        document.getElementById("fb-database-url").value = firebaseRestUrl;
        fbModal.classList.remove("hidden");
    };
    document.getElementById("open-firebase-btn").addEventListener("click", openFbModal);
    document.getElementById("open-firebase-setup-from-login").addEventListener("click", openFbModal);
    document.getElementById("close-firebase-modal").addEventListener("click", () => fbModal.classList.add("hidden"));

    document.getElementById("save-firebase-btn").addEventListener("click", () => {
        const urlInput = document.getElementById("fb-database-url").value.trim();
        const jsonInput = document.getElementById("fb-config-json").value.trim();
        const chosen = urlInput || jsonInput;
        firebaseRestUrl = normalizeFirebaseUrl(chosen);
        localStorage.setItem("ky_fb_url", firebaseRestUrl);
        fbModal.classList.add("hidden");
        setupFirebaseCloudSync(currentRoom);
        updateSyncBadgeUI();
        showToast(firebaseRestUrl ? "☁️ Firebase Bulut bağlantısı etkinleştirildi!" : "Yerel ortak senkronizasyon aktif");
    });

    document.getElementById("clear-firebase-btn").addEventListener("click", () => {
        firebaseRestUrl = "";
        localStorage.removeItem("ky_fb_url");
        document.getElementById("fb-database-url").value = "";
        document.getElementById("fb-config-json").value = "";
        fbModal.classList.add("hidden");
        setupFirebaseCloudSync(currentRoom);
        updateSyncBadgeUI();
        showToast("Varsayılan senkronizasyon moduna dönüldü");
    });
}

function applySessionHeader() {
    document.getElementById("room-badge").textContent = `Oda: ${currentRoom}`;
    document.getElementById("active-partner-name").textContent = currentPartner;
    const parts = (currentPartner || "O").trim().split(/\s+/);
    const initials = parts.length > 1 ? (parts[0][0] + parts[1][0]).toUpperCase() : (parts[0][0] || "O").toUpperCase();
    document.getElementById("partner-avatar").textContent = initials;
}

function openProductModal(productId = null) {
    const modal = document.getElementById("product-modal");
    const title = document.getElementById("product-modal-title");

    if (productId) {
        const p = storeData.products.find((x) => x.id === productId);
        if (!p) return;
        title.textContent = "Ürünü Düzenle";
        document.getElementById("product-id").value = p.id;
        document.getElementById("prod-name").value = p.name;
        document.getElementById("prod-category").value = p.category;
        document.getElementById("prod-unit").value = p.unit;
        document.getElementById("prod-waste").value = p.wastePercent;
        document.getElementById("prod-cost").value = p.costPrice;
        document.getElementById("prod-price").value = p.targetPrice;
    } else {
        title.textContent = "Yeni Kuruyemiş / Ürün Ekle";
        document.getElementById("product-form").reset();
        document.getElementById("product-id").value = "";
        document.getElementById("prod-waste").value = 2;
    }

    updateModalLivePreview();
    modal.classList.remove("hidden");
}

function closeProductModal() {
    document.getElementById("product-modal").classList.add("hidden");
}

function updateModalLivePreview() {
    const cost = Number(document.getElementById("prod-cost").value) || 0;
    const price = Number(document.getElementById("prod-price").value) || 0;
    const waste = Number(document.getElementById("prod-waste").value) || 0;

    const m = calculateProductMetrics({
        costPrice: cost,
        targetPrice: price,
        wastePercent: waste,
        marketPrice: 0,
        dailySimQty: 1
    });

    document.getElementById("preview-real-cost").textContent = formatTL(m.realCost);
    const unitProfitEl = document.getElementById("preview-unit-profit");
    unitProfitEl.textContent = (m.unitProfit >= 0 ? "+" : "") + formatTL(m.unitProfit);
    unitProfitEl.className = m.unitProfit >= 0 ? "text-success" : "text-danger";

    const marginEl = document.getElementById("preview-margin");
    marginEl.textContent = `%${m.grossMargin.toFixed(1)}`;
    marginEl.className = `badge-margin ${m.grossMargin >= 30 ? "high" : m.grossMargin >= 18 ? "medium" : "low"}`;
}

// Başlat
window.addEventListener("DOMContentLoaded", setupEventListeners);
