require('dotenv').config();
const express      = require('express');
const { Telegraf } = require('telegraf');
const { createClient } = require('@supabase/supabase-js');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// ─────────────────────────────────────────────────────────────
// 1. VALIDATE ENVIRONMENT
// ─────────────────────────────────────────────────────────────
const REQUIRED_ENV = [
  'TELEGRAM_BOT_TOKEN',
  'SUPABASE_URL',
  'SUPABASE_SERVICE_ROLE_KEY',
  'GEMINI_API_KEY',
];
for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    console.error(`❌  Missing env variable: ${key}`);
    process.exit(1);
  }
}

// ─────────────────────────────────────────────────────────────
// 2. INITIALIZE CLIENTS
// ─────────────────────────────────────────────────────────────
const bot = new Telegraf(process.env.TELEGRAM_BOT_TOKEN);

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { autoRefreshToken: false, persistSession: false } }
);

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// gemini-2.5-flash: confirmed available on this API key.
// systemInstruction is built dynamically per-user (see buildSystemInstruction).
// responseMimeType enforces strict JSON output — no fences, no prose.
const MODEL_NAME = 'gemini-2.5-flash';

function buildSystemInstruction(targetGoal, currentStep) {
  return (
    `You are a strict mentor. The user's goal is "${targetGoal}" and they are currently on ` +
    `step/lesson ${currentStep}. Do NOT ask questions outside this scope. ` +
    `Evaluate the user's answer based on this context. ` +
    `Return ONLY a JSON object with exactly two keys: ` +
    `"feedback" (string: your educational response and the next question for step ${currentStep}) ` +
    `and "score" (integer 0–10 evaluating the quality of their answer). ` +
    `Do not include any text, markdown, or code blocks outside the JSON object. ` +
    `CRITICAL LANGUAGE RULE: You are bilingual. You MUST instantly adapt to the user's language. ` +
    `If the user types in Arabic or requests Arabic, you MUST respond entirely in clear, professional Arabic. ` +
    `However, you MUST keep all cybersecurity technical terms (e.g., CIA Triad, GRC, Malware, Phishing, Firewall, Encryption) ` +
    `in English to preserve the integrity of the Security+ curriculum.`
  );
}

// ─────────────────────────────────────────────────────────────
// CONVERSATIONAL MEMORY — last 4 message pairs per telegram_id
// Stored as arrays of Gemini history objects:
//   { role: 'user'|'model', parts: [{ text }] }
// The Map is in-process only; it resets on deploy (acceptable for MVP).
// ─────────────────────────────────────────────────────────────
const MAX_HISTORY = 4; // message pairs to retain per user
/** @type {Map<number, Array<{ role: string, parts: Array<{ text: string }> }>>} */
const chatHistories = new Map();

// ─────────────────────────────────────────────────────────────
// 3. ROADMAP HELPER
// ─────────────────────────────────────────────────────────────

/**
 * Fetch the user's roadmap row from user_roadmaps.
 * If no row exists, insert one with default goal 'Security+' at step 1.
 *
 * @param {number} telegramId
 * @returns {Promise<{ target_goal: string, current_step: number }>}
 */
async function getRoadmap(telegramId) {
  const { data, error } = await supabase
    .from('user_roadmaps')
    .select('target_goal, current_step')
    .eq('telegram_id', telegramId)
    .single();

  if (data) return data;

  // No row found (PGRST116) or other error — upsert default
  const defaults = { target_goal: 'Security+', current_step: 1 };
  const { data: inserted, error: insertErr } = await supabase
    .from('user_roadmaps')
    .upsert({ telegram_id: telegramId, ...defaults }, { onConflict: 'telegram_id' })
    .select('target_goal, current_step')
    .single();

  if (insertErr) {
    console.error('[getRoadmap] Failed to upsert default roadmap:', insertErr.message);
    return defaults; // non-fatal: use in-memory defaults
  }
  return inserted;
}

// ─────────────────────────────────────────────────────────────
// 4. THE GEMINI SHIELD — RATE-LIMIT AWARE MESSAGE QUEUE
//
//    Free tier allows ~10–15 requests/min (≈1 req/6 s).
//    Each queue item waits QUEUE_DELAY_MS before the next
//    one begins, smoothing bursts into a controlled stream.
// ─────────────────────────────────────────────────────────────
const QUEUE_DELAY_MS = 5_000;
const MAX_QUEUE_SIZE = 50;

/** @type {Array<{ ctx: import('telegraf').Context, telegramId: number, userMessage: string }>} */
const messageQueue = [];
let isProcessing   = false;

async function processQueue() {
  if (isProcessing || messageQueue.length === 0) return;
  isProcessing = true;

  const { ctx, telegramId, userMessage } = messageQueue.shift();

  try {
    // ── Step A: Security check ───────────────────────────────
    const { data: user, error: dbError } = await supabase
      .from('users')
      .select('id, full_name, goal, level, daily_time, is_active')
      .eq('telegram_id', telegramId)
      .single();

    if (dbError || !user) {
      await ctx.reply(
        '⛔ Your account is not registered or has been suspended by the admin.\n' +
        'Please start from the website to create a new profile.'
      );
      isProcessing = false;
      setTimeout(processQueue, 1_000);
      return;
    }

    if (!user.is_active) {
      await ctx.reply('⛔ Your account has been deactivated. Contact support if this is a mistake.');
      isProcessing = false;
      setTimeout(processQueue, 1_000);
      return;
    }

    // ── Step B: Fetch roadmap (upserts default if missing) ───
    const roadmap = await getRoadmap(telegramId);
    const { target_goal, current_step } = roadmap;

    // ── Step C: Build per-request Gemini model with dynamic SI
    const requestModel = genAI.getGenerativeModel({
      model: MODEL_NAME,
      systemInstruction: buildSystemInstruction(target_goal, current_step),
      generationConfig:  { responseMimeType: 'application/json' },
    });

    // ── Step D: Retrieve / initialise this user's chat history
    if (!chatHistories.has(telegramId)) {
      chatHistories.set(telegramId, []);
    }
    const history = chatHistories.get(telegramId);

    // Start a chat session with the existing history
    const chat = requestModel.startChat({ history });

    // ── Step E: Send message, receive structured JSON ─────────
    const result  = await chat.sendMessage(buildPrompt(user, userMessage, target_goal, current_step));
    const rawText = result.response.text();

    let feedback, score;
    try {
      ({ feedback, score } = parseGeminiJSON(rawText));
    } catch (parseErr) {
      console.error('━━━ [Queue] Gemini JSON parse failure ━━━');
      console.error('Error  :', parseErr.message);
      console.error('Raw    :', JSON.stringify(rawText));
      console.error('Cleaned:', JSON.stringify(stripMarkdownFences(rawText)));
      console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      await ctx.reply(
        rawText && rawText.trim().length > 0
          ? rawText.trim()
          : '⚠️ I had trouble formatting my response. Please try again.'
      );
      isProcessing = false;
      setTimeout(processQueue, QUEUE_DELAY_MS);
      return;
    }

    // ── Step F: Update in-memory chat history (cap at MAX_HISTORY pairs)
    history.push(
      { role: 'user',  parts: [{ text: userMessage }] },
      { role: 'model', parts: [{ text: rawText       }] }
    );
    // Keep only the last MAX_HISTORY pairs (each pair = 2 entries)
    if (history.length > MAX_HISTORY * 2) {
      history.splice(0, history.length - MAX_HISTORY * 2);
    }

    // ── Step G: Send only the feedback text to the student ───
    await ctx.reply(feedback);

    // ── Step H: Silently log score to progress_logs ──────────
    await logUserProgress(telegramId, score, extractTopic(userMessage));

    // ── Step I: Update the main progress table ───────────────
    await supabase.from('progress').insert({
      user_id:    user.id,
      topic:      extractTopic(userMessage),
      evaluation: `Score: ${score}/10 | Goal: ${target_goal} | Step: ${current_step}`,
      mistakes:   [],
      timestamp:  new Date().toISOString(),
    });

  } catch (err) {
    console.error('[Queue] Processing error:', err?.message ?? err);

    const isRateLimit = err?.message?.includes('429') || err?.status === 429;
    if (isRateLimit) {
      await ctx.reply(
        '⏳ Fox is a little busy right now (too many lessons at once!).\n' +
        'Your request is queued — try again in 30 seconds.'
      );
      messageQueue.unshift({ ctx, telegramId, userMessage });
      isProcessing = false;
      setTimeout(processQueue, 30_000);
      return;
    }

    await ctx.reply('⚠️ A technical error occurred. Please try again in a moment.');
  }

  isProcessing = false;
  setTimeout(processQueue, QUEUE_DELAY_MS);
}

// ─────────────────────────────────────────────────────────────
// 4. PROMPT BUILDER
// ─────────────────────────────────────────────────────────────
function buildPrompt(user, userMessage, targetGoal, currentStep) {
  const dailyMin = user.daily_time ?? 10;
  return `Student Profile:
- Name: ${user.full_name}
- Level: ${user.level}
- Daily budget: ${dailyMin} minutes
- Current goal: ${targetGoal}
- Current lesson/step: ${currentStep}

Student's message: "${userMessage}"

Evaluate the student's message in the context of step ${currentStep} of ${targetGoal}. Reply with only a valid JSON object with keys "feedback" and "score".`;
}

// ─────────────────────────────────────────────────────────────
// 5. HELPERS
// ─────────────────────────────────────────────────────────────

function extractTopic(message) {
  return message.slice(0, 80).trim();
}

/**
 * Strip markdown code-block fences from a Gemini response string.
 * Handles ```json ... ```, ``` ... ```, and single-backtick wrapping.
 */
function stripMarkdownFences(raw) {
  let s = raw.trim();
  const tripleMatch = s.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  if (tripleMatch) {
    s = tripleMatch[1].trim();
  } else if (s.startsWith('`') && s.endsWith('`')) {
    s = s.slice(1, -1).trim();
  }
  return s;
}

/**
 * Parse raw Gemini output into { feedback: string, score: number }.
 * stripMarkdownFences runs first so JSON.parse always receives clean input.
 */
function parseGeminiJSON(raw) {
  const cleaned = stripMarkdownFences(raw);
  const parsed  = JSON.parse(cleaned);

  if (typeof parsed.feedback !== 'string' || parsed.feedback.trim() === '') {
    throw new Error('Gemini JSON missing "feedback" string.');
  }
  const score = parseInt(parsed.score, 10);
  if (isNaN(score) || score < 0 || score > 10) {
    throw new Error(`Gemini JSON "score" out of range: ${parsed.score}`);
  }
  return { feedback: parsed.feedback.trim(), score };
}

/**
 * Silently insert a scored session into progress_logs.
 * Failures are logged server-side and never reach the student.
 */
async function logUserProgress(telegramId, score, topic) {
  try {
    const { error } = await supabase
      .from('progress_logs')
      .insert({
        telegram_id: telegramId,
        score,
        topic:      topic.slice(0, 120),
        created_at: new Date().toISOString(),
      });
    if (error) throw error;
  } catch (err) {
    console.error('[logUserProgress] Failed to log score:', err?.message ?? err);
  }
}

/** Fetch active user by telegram_id — returns null if not found or banned. */
async function getActiveUser(telegramId) {
  const { data, error } = await supabase
    .from('users')
    .select('id, full_name, goal, level, is_active')
    .eq('telegram_id', telegramId)
    .single();
  if (error || !data || !data.is_active) return null;
  return data;
}

// ─────────────────────────────────────────────────────────────
// 6. /start — DEEP LINKING (website → Telegram account claim)
// ─────────────────────────────────────────────────────────────
bot.start(async (ctx) => {
  const supabaseUserId = ctx.startPayload;
  const telegramId     = ctx.from.id;

  if (!supabaseUserId) {
    return ctx.reply(
      `👋 Welcome to *Fox AI*!\n\n` +
      `To start learning, visit our website and complete the 60-second onboarding.\n` +
      `You'll be sent directly back here with your personalised plan. 🚀`,
      { parse_mode: 'Markdown' }
    );
  }

  try {
    const existingUser = await getActiveUser(telegramId);
    if (existingUser) {
      return ctx.reply(
        `✅ Your account is already linked, *${existingUser.full_name}*!\n\n` +
        `Just send me a message to continue learning about *${existingUser.goal}*. 🎯`,
        { parse_mode: 'Markdown' }
      );
    }

    const { data, error } = await supabase
      .from('users')
      .update({ telegram_id: telegramId, is_active: true })
      .eq('id', supabaseUserId)
      .select('full_name, goal, level, daily_time')
      .single();

    if (error || !data) {
      console.error('[/start] Link error:', error?.message);
      return ctx.reply(
        "❌ I couldn't find your onboarding profile. Please go back to the website and try again.\n" +
        'The link may have expired or already been used.'
      );
    }

    const levelEmoji = { beginner: '🌱', intermediate: '📚', advanced: '🚀' }[data.level] ?? '📖';

    await ctx.reply(
      `🎉 *Account linked successfully, ${data.full_name}!*\n\n` +
      `Here's your study plan:\n` +
      `• 🎯 Goal: *${data.goal}*\n` +
      `• ${levelEmoji} Level: *${data.level}*\n` +
      `• ⏱ Daily time: *${data.daily_time} minutes*\n\n` +
      `Send me any question or topic related to your goal and I'll start coaching you.\n` +
      `Type *"Start"* to get your very first lesson! 🦊`,
      { parse_mode: 'Markdown' }
    );
  } catch (err) {
    console.error('[/start] Unexpected error:', err?.message ?? err);
    ctx.reply('⚠️ An internal error occurred during account linking. Please try again.');
  }
});

// ─────────────────────────────────────────────────────────────
// 7. /progress — show the user their session history
// ─────────────────────────────────────────────────────────────
bot.command('progress', async (ctx) => {
  const telegramId = ctx.from.id;
  const user = await getActiveUser(telegramId);
  if (!user) return ctx.reply('⛔ Account not found. Visit the website to register.');

  const { data: sessions, error } = await supabase
    .from('progress')
    .select('topic, evaluation, timestamp')
    .eq('user_id', user.id)
    .order('timestamp', { ascending: false })
    .limit(10);

  if (error || !sessions?.length) {
    return ctx.reply("You haven't completed any sessions yet. Send me a message to start! 🚀");
  }

  const lines = sessions.map((s, i) => {
    const date = new Date(s.timestamp).toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
    return `${i + 1}. *${s.topic.slice(0, 40)}* — ${date}`;
  });

  await ctx.reply(
    `📊 *Your last ${sessions.length} sessions, ${user.full_name}:*\n\n` +
    lines.join('\n') +
    '\n\nKeep it up! Consistency is everything. 💪',
    { parse_mode: 'Markdown' }
  );
});

// ─────────────────────────────────────────────────────────────
// 8. /help — command reference
// ─────────────────────────────────────────────────────────────
bot.command('help', (ctx) => {
  ctx.reply(
    `🦊 *Fox AI — Commands*\n\n` +
    `/start — Link your website account\n` +
    `/progress — View your last 10 sessions\n` +
    `/help — Show this message\n\n` +
    `Or just *send any message* and Fox will coach you instantly.`,
    { parse_mode: 'Markdown' }
  );
});

// ─────────────────────────────────────────────────────────────
// 9. HANDLE INCOMING TEXT MESSAGES
// ─────────────────────────────────────────────────────────────
bot.on('text', async (ctx) => {
  const telegramId  = ctx.from.id;
  const userMessage = ctx.message.text.trim();

  if (!userMessage) return;

  if (messageQueue.length >= MAX_QUEUE_SIZE) {
    return ctx.reply('⚠️ Fox is under heavy load right now. Please wait a moment and try again.');
  }

  const position = messageQueue.length + (isProcessing ? 1 : 0);
  const waitMsg  = position > 0
    ? `⏳ Analysing... (you are #${position + 1} in the queue)`
    : '⏳ Analysing your message...';

  await ctx.reply(waitMsg);
  messageQueue.push({ ctx, telegramId, userMessage });
  processQueue();
});

// ─────────────────────────────────────────────────────────────
// 10. GLOBAL ERROR HANDLERS
// ─────────────────────────────────────────────────────────────
bot.catch((err, ctx) => {
  console.error(`[Telegraf] Unhandled error for update ${ctx.updateType}:`, err?.message ?? err);
});

process.on('unhandledRejection', (reason) => {
  console.error('[Process] Unhandled rejection:', reason);
});

// ─────────────────────────────────────────────────────────────
// 11. HEALTH SERVER — required by Render Free Web Service
// ─────────────────────────────────────────────────────────────
const app  = express();
const PORT = process.env.PORT || 3000;

app.get('/', (_req, res) => {
  res.send('🤖 Fox Bot is running — Telegraf polling is active.');
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🌐 Health server listening on 0.0.0.0:${PORT}`);
});

// ─────────────────────────────────────────────────────────────
// 12. LAUNCH — POLLING MODE
// ─────────────────────────────────────────────────────────────
bot.telegram.deleteWebhook({ drop_pending_updates: true })
  .then(() => console.log('✅ Webhook deleted — ghost sessions cleared.'))
  .catch((e) => console.warn('⚠️  deleteWebhook failed (non-fatal):', e.message));

bot.launch({
  allowedUpdates: ['message', 'callback_query'],
  dropPendingUpdates: true,
})
  .then(() => {
    console.log('🤖 Fox Telegram Bot Worker running in Polling mode...');
    console.log(`📋 Queue capacity: ${MAX_QUEUE_SIZE} messages`);
    console.log(`⏱  Gemini rate-limit delay: ${QUEUE_DELAY_MS / 1000}s between calls`);
  })
  .catch((err) => {
    console.error('❌ Failed to launch bot:', err?.message ?? err);
    process.exit(1);
  });

// Graceful shutdown — prevents 409 Conflict on Render redeploys
process.once('SIGINT',  () => { console.log('Shutting down (SIGINT)…');  bot.stop('SIGINT');  });
process.once('SIGTERM', () => { console.log('Shutting down (SIGTERM)…'); bot.stop('SIGTERM'); });
